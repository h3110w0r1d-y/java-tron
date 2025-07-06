package org.tron.plugins;

import java.io.File;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import me.tongfei.progressbar.ProgressBar;
import org.apache.commons.collections4.CollectionUtils;
import org.rocksdb.RocksDBException;
import org.tron.common.bloom.Bloom;
import org.tron.common.utils.ByteArray;
import org.tron.common.utils.ByteUtil;
import org.tron.core.capsule.TransactionRetCapsule;
import org.tron.core.exception.BadItemException;
import org.tron.core.exception.EventBloomException;
import org.tron.plugins.utils.db.DBInterface;
import org.tron.plugins.utils.db.DbTool;
import picocli.CommandLine;

@Slf4j(topic = "backfill-bloom")
@CommandLine.Command(name = "backfill-bloom", description = "Backfill SectionBloom data for historical blocks to enable eth_getLogs address filtering.", exitCodeListHeading = "Exit Codes:%n", exitCodeList = {
    "0:Successful",
    "1:Internal error: exception occurred, please check toolkit.log" })
public class DbBackfillBloom implements Callable<Integer> {

  @CommandLine.Spec
  CommandLine.Model.CommandSpec spec;

  @CommandLine.Option(names = { "--database-directory",
      "-d" }, defaultValue = "output-directory/database", description = "Database directory path. Default: ${DEFAULT-VALUE}", order = 1)
  private String databaseDirectory;

  @CommandLine.Option(names = { "--start-block",
      "-s" }, required = true, description = "Start block number for backfill", order = 2)
  private long startBlock;

  @CommandLine.Option(names = { "--end-block",
      "-e" }, description = "End block number for backfill (default: latest block)", order = 3)
  private Long endBlock;

  // 固定批次大小为一个 section (2048 blocks)，确保每个线程处理不同的 section
  private static final int BATCH_SIZE = 2048;
  private static final int BLOCKS_PER_SECTION = 2048;

  @CommandLine.Option(names = { "--max-concurrency",
      "-c" }, defaultValue = "5", description = "Maximum concurrency for processing. Default: ${DEFAULT-VALUE}", order = 5)
  private int maxConcurrency;

  @CommandLine.Option(names = { "--force-flush",
      "-f" }, defaultValue = "true", description = "Force database flush after each batch. Default: ${DEFAULT-VALUE}", order = 6)
  private boolean forceFlush;

  @CommandLine.Option(names = { "--help", "-h" }, help = true, description = "Display help message", order = 7)
  private boolean help;

  // Statistics
  private final AtomicLong processedBlocks = new AtomicLong(0); // 已遍历的区块数（包括失败的）
  private final AtomicLong successfulBlocks = new AtomicLong(0); // 成功处理的区块数
  private final AtomicLong blocksWithLogs = new AtomicLong(0); // 包含日志的区块数
  private final AtomicLong errorCount = new AtomicLong(0); // 处理失败的区块数

  // 性能监控（无需锁，因为每个线程处理不同的 section）
  private final AtomicLong totalBloomWrites = new AtomicLong(0);

  // Section 范围类
  private static class SectionRange {
    final long start;
    final long end;
    final int sectionId;

    SectionRange(long start, long end, int sectionId) {
      this.start = start;
      this.end = end;
      this.sectionId = sectionId;
    }

    @Override
    public String toString() {
      return String.format("Section %d: [%d-%d]", sectionId, start, end);
    }
  }

  @Override
  public Integer call() {
    if (help) {
      spec.commandLine().usage(System.out);
      return 0;
    }

    try {
      // Validate parameters
      if (!validateParameters()) {
        return 1;
      }

      // Initialize database connections
      if (!initializeDatabase()) {
        return 1;
      }

      // Determine end block if not specified
      if (endBlock == null) {
        endBlock = getLatestBlockNumber();
        if (endBlock == null) {
          spec.commandLine().getErr().println("Failed to determine latest block number");
          return 1;
        }
      }

      // Validate block range
      if (endBlock < startBlock) {
        spec.commandLine().getErr().println("End block must be >= start block");
        return 1;
      }

      long totalBlocks = endBlock - startBlock + 1;
      spec.commandLine().getOut().printf("Starting SectionBloom backfill for blocks %d to %d (%d blocks)%n",
          startBlock, endBlock, totalBlocks);

      // Process blocks with progress bar
      long startTime = System.currentTimeMillis();
      int result = processBlocks();
      long duration = (System.currentTimeMillis() - startTime) / 1000;

      // Print summary
      printSummary(duration);

      return result;

    } catch (Exception e) {
      logger.error("Backfill failed", e);
      spec.commandLine().getErr().println("Backfill failed: " + e.getMessage());
      return 1;
    } finally {
      DbTool.close();
    }
  }

  private boolean validateParameters() {
    if (startBlock < 0) {
      spec.commandLine().getErr().println("Start block must be >= 0");
      return false;
    }

    if (maxConcurrency <= 0 || maxConcurrency > 128) {
      spec.commandLine().getErr().println("Max concurrency must be between 1 and 128");
      return false;
    }

    File dbDir = new File(databaseDirectory);
    if (!dbDir.exists() || !dbDir.isDirectory()) {
      spec.commandLine().getErr().println("Database directory does not exist: " + databaseDirectory);
      return false;
    }

    return true;
  }

  private boolean initializeDatabase() {
    try {
      // Initialize database connections
      DbTool.getDB(databaseDirectory, "transactionRetStore");
      DbTool.getDB(databaseDirectory, "section-bloom");

      spec.commandLine().getOut().println("Database connections initialized successfully");
      return true;
    } catch (Exception e) {
      logger.error("Failed to initialize database connections", e);
      spec.commandLine().getErr().println("Failed to initialize database: " + e.getMessage());
      return false;
    }
  }

  private Long getLatestBlockNumber() {
    try {
      DBInterface blockIndexDb = DbTool.getDB(databaseDirectory, "block_index");
      byte[] latestBlockKey = "latest_block_header_number".getBytes();
      byte[] latestBlockBytes = blockIndexDb.get(latestBlockKey);

      if (latestBlockBytes != null) {
        return ByteArray.toLong(latestBlockBytes);
      }
      return null;
    } catch (Exception e) {
      logger.error("Failed to get latest block number", e);
      return null;
    }
  }

  private int processBlocks() {
    long totalBlocks = endBlock - startBlock + 1;
    ExecutorService executor = Executors.newFixedThreadPool(maxConcurrency);
    List<CompletableFuture<Void>> futures = new ArrayList<>();

    try (ProgressBar pb = new ProgressBar("Scanning blocks for SectionBloom backfill", totalBlocks)) {

      // 计算需要处理的 section 范围
      List<SectionRange> sectionRanges = calculateSectionRanges(startBlock, endBlock);

      spec.commandLine().getOut().printf("Processing %d sections with %d threads\n",
          sectionRanges.size(), maxConcurrency);
      // 提交所有 section 任务到线程池，每个线程处理一个完整的 section
      for (SectionRange range : sectionRanges) {
        final long finalSectionStart = range.start;
        final long finalSectionEnd = range.end;

        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
          try {
            processSection(finalSectionStart, finalSectionEnd, pb);
          } catch (Exception e) {
            spec.commandLine().getOut().printf("Error processing section %d to %d, %s\n",
                finalSectionStart, finalSectionEnd, e);
            // 注意：section 内部的错误已经在 processSection 中统计了
          }
        }, executor);

        futures.add(future);
      }

      // 等待所有任务完成
      CompletableFuture<Void> allTasks = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));

      try {
        allTasks.get(); // 等待所有任务完成
        spec.commandLine().getOut().printf("All %d batch tasks completed\n", futures.size());
      } catch (Exception e) {
        spec.commandLine().getOut().printf("Error waiting for tasks to complete: %s\n", e.getMessage());
        return 1;
      }

    } catch (Exception e) {
      spec.commandLine().getOut().printf("Error in progress tracking %s\n", e);
      return 1;
    } finally {
      // 关闭线程池
      executor.shutdown();
      try {
        if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
          spec.commandLine().getOut().println("Forcing shutdown of executor...");
          executor.shutdownNow();
        }
      } catch (InterruptedException e) {
        executor.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }

    return errorCount.get() > 0 ? 1 : 0;
  }

  /**
   * 计算需要处理的 section 范围，确保每个线程处理完整的 section
   * 例如：startBlock=1000, endBlock=4000 会生成：
   * - Section 0: [1000-2047] (section 0 的剩余部分)
   * - Section 1: [2048-4095] (完整的 section 1，但只处理到 4000)
   */
  private List<SectionRange> calculateSectionRanges(long startBlock, long endBlock) {
    List<SectionRange> ranges = new ArrayList<>();

    long currentBlock = startBlock;
    while (currentBlock <= endBlock) {
      // 计算当前区块所属的 section
      int sectionId = (int) (currentBlock / BLOCKS_PER_SECTION);

      // 计算这个 section 的边界
      long sectionStart = sectionId * BLOCKS_PER_SECTION;
      long sectionEnd = sectionStart + BLOCKS_PER_SECTION - 1;

      // 调整为实际需要处理的范围
      long rangeStart = Math.max(currentBlock, sectionStart);
      long rangeEnd = Math.min(endBlock, sectionEnd);

      ranges.add(new SectionRange(rangeStart, rangeEnd, sectionId));

      // 移动到下一个 section
      currentBlock = sectionEnd + 1;
    }

    return ranges;
  }

  private void processSection(long sectionStart, long sectionEnd, ProgressBar pb) {
    int sectionId = (int) (sectionStart / BLOCKS_PER_SECTION);
    try {
      DBInterface transactionRetDb = DbTool.getDB(databaseDirectory, "transactionRetStore");
      DBInterface sectionBloomDb = DbTool.getDB(databaseDirectory, "section-bloom");

      for (long blockNum = sectionStart; blockNum <= sectionEnd; blockNum++) {
        // 无论处理结果如何，都更新进度
        long currentProcessed = processedBlocks.incrementAndGet();
        pb.step();

        try {
          processBlock(blockNum, transactionRetDb, sectionBloomDb);
          successfulBlocks.incrementAndGet();
        } catch (Exception e) {
          spec.commandLine().getOut().printf("Error processing block %d, %s\n", blockNum, e);
          errorCount.incrementAndGet();
        }
      }

    } catch (Exception e) {
      spec.commandLine().getOut().printf("Error in section %d processing: %s\n", sectionId, e);
      throw new RuntimeException(e);
    }
  }

  private void processBlock(long blockNum, DBInterface transactionRetDb, DBInterface sectionBloomDb)
      throws RocksDBException, BadItemException, EventBloomException {

    // Get transaction info for this block
    byte[] blockKey = ByteArray.fromLong(blockNum);
    byte[] transactionRetData = transactionRetDb.get(blockKey);

    if (transactionRetData == null) {
      return;
    }

    try {
      TransactionRetCapsule transactionRetCapsule = new TransactionRetCapsule(transactionRetData);

      // Create bloom filter for this block using the same logic as SectionBloomStore
      Bloom blockBloom = Bloom.createBloom(transactionRetCapsule);

      if (blockBloom != null) {
        // Extract bit positions from bloom filter
        List<Integer> bitList = extractBitPositions(blockBloom);

        if (!CollectionUtils.isEmpty(bitList)) {
          // Write to section bloom store using the same logic as SectionBloomStore.write
          writeSectionBloom(blockNum, bitList, sectionBloomDb);
          blocksWithLogs.incrementAndGet();
        }
      }
    } catch (Exception e) {
      spec.commandLine().getOut().printf("Error processing block %d: %s\n", blockNum, e.getMessage());
      throw e;
    }
  }

  private List<Integer> extractBitPositions(Bloom blockBloom) {
    List<Integer> bitList = new ArrayList<>();
    BitSet bs = BitSet.valueOf(blockBloom.getData());
    for (int i = bs.nextSetBit(0); i >= 0; i = bs.nextSetBit(i + 1)) {
      // operate on index i here
      if (i == Integer.MAX_VALUE) {
        break; // or (i+1) would overflow
      }
      bitList.add(i);
    }
    return bitList;
  }

  private void writeSectionBloom(long blockNum, List<Integer> bitList, DBInterface sectionBloomDb)
      throws RocksDBException, EventBloomException {

    int section = (int) (blockNum / BLOCKS_PER_SECTION);
    int blockNumOffset = (int) (blockNum % BLOCKS_PER_SECTION);

    for (int bitIndex : bitList) {
      // 无需锁：每个线程处理不同的 section，不会有并发冲突
      // Get existing BitSet from database
      BitSet bitSet = getSectionBloomBitSet(section, bitIndex, sectionBloomDb);
      if (Objects.isNull(bitSet)) {
        bitSet = new BitSet(BLOCKS_PER_SECTION);
      }

      // Update the bit for this block
      bitSet.set(blockNumOffset);

      // Put back into database
      putSectionBloomBitSet(section, bitIndex, bitSet, sectionBloomDb);
      totalBloomWrites.incrementAndGet();
    }
  }

  private long combineKey(int section, int bitIndex) {
    return section * 1_000_000L + bitIndex;
  }

  private BitSet getSectionBloomBitSet(int section, int bitIndex, DBInterface sectionBloomDb)
      throws EventBloomException {
    long keyLong = combineKey(section, bitIndex);
    byte[] key = Long.toHexString(keyLong).getBytes();
    byte[] data = sectionBloomDb.get(key);

    if (data == null) {
      return null;
    }

    try {
      byte[] decompressedData = ByteUtil.decompress(data);
      return BitSet.valueOf(decompressedData);
    } catch (Exception e) {
      throw new EventBloomException("decompress byte failed");
    }
  }

  private void putSectionBloomBitSet(int section, int bitIndex, BitSet bitSet, DBInterface sectionBloomDb)
      throws RocksDBException, EventBloomException {
    long keyLong = combineKey(section, bitIndex);
    byte[] key = Long.toHexString(keyLong).getBytes();

    try {
      byte[] compressedData = ByteUtil.compress(bitSet.toByteArray());
      sectionBloomDb.put(key, compressedData);
    } catch (Exception e) {
      throw new EventBloomException("compress byte failed");
    }
  }

  private void printSummary(long duration) {
    spec.commandLine().getOut().println("\n=== Backfill Summary ===");

    // 基本统计
    spec.commandLine().getOut().printf("Total blocks scanned: %d%n", processedBlocks.get());
    spec.commandLine().getOut().printf("Successfully processed: %d%n", successfulBlocks.get());
    spec.commandLine().getOut().printf("Blocks with logs: %d%n", blocksWithLogs.get());
    spec.commandLine().getOut().printf("Errors encountered: %d%n", errorCount.get());
    spec.commandLine().getOut().printf("Duration: %d seconds%n", duration);

    // 成功率统计
    if (processedBlocks.get() > 0) {
      double successRate = (double) successfulBlocks.get() / processedBlocks.get() * 100;
      double logRate = (double) blocksWithLogs.get() / processedBlocks.get() * 100;
      spec.commandLine().getOut().printf("Success rate: %.2f%% (%d/%d)%n",
          successRate, successfulBlocks.get(), processedBlocks.get());
      spec.commandLine().getOut().printf("Blocks with logs rate: %.2f%% (%d/%d)%n",
          logRate, blocksWithLogs.get(), processedBlocks.get());
    }

    // 性能统计
    spec.commandLine().getOut().printf("Total bloom writes: %d%n", totalBloomWrites.get());
    spec.commandLine().getOut().printf("Max concurrency used: %d threads%n", maxConcurrency);
    spec.commandLine().getOut().printf("Section-based processing: No locks needed%n");

    if (duration > 0) {
      spec.commandLine().getOut().printf("Scanning rate: %.2f blocks/second%n",
          (double) processedBlocks.get() / duration);
      spec.commandLine().getOut().printf("Processing rate: %.2f blocks/second%n",
          (double) successfulBlocks.get() / duration);
      if (totalBloomWrites.get() > 0) {
        spec.commandLine().getOut().printf("Bloom write rate: %.2f writes/second%n",
            (double) totalBloomWrites.get() / duration);
      }
    }

    // 结果判断
    if (errorCount.get() == 0) {
      spec.commandLine().getOut().println("✓ Backfill completed successfully!");
    } else {
      spec.commandLine().getOut().printf("⚠ Backfill completed with %d errors. Check output above for details.%n",
          errorCount.get());
    }
  }
}
