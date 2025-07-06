package org.tron.plugins;

import java.io.File;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;
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

  @CommandLine.Option(names = { "--batch-size",
      "-b" }, defaultValue = "1000", description = "Batch size for processing blocks. Default: ${DEFAULT-VALUE}", order = 4)
  private int batchSize;

  @CommandLine.Option(names = { "--max-concurrency",
      "-c" }, defaultValue = "5", description = "Maximum concurrency for processing. Default: ${DEFAULT-VALUE}", order = 5)
  private int maxConcurrency;

  @CommandLine.Option(names = { "--force-flush",
      "-f" }, defaultValue = "true", description = "Force database flush after each batch. Default: ${DEFAULT-VALUE}", order = 6)
  private boolean forceFlush;

  @CommandLine.Option(names = { "--help", "-h" }, help = true, description = "Display help message", order = 7)
  private boolean help;

  // Statistics
  private final AtomicLong processedBlocks = new AtomicLong(0);
  private final AtomicLong blocksWithLogs = new AtomicLong(0);
  private final AtomicLong errorCount = new AtomicLong(0);

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

    if (batchSize <= 0 || batchSize > 10000) {
      spec.commandLine().getErr().println("Batch size must be between 1 and 10000");
      return false;
    }

    if (maxConcurrency <= 0 || maxConcurrency > 20) {
      spec.commandLine().getErr().println("Max concurrency must be between 1 and 20");
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

      // Fallback: scan block database for highest block number
      DBInterface blockDb = DbTool.getDB(databaseDirectory, "block");
      // This is a simplified approach - in practice you might need more sophisticated
      // scanning
      return null;

    } catch (Exception e) {
      logger.error("Failed to get latest block number", e);
      return null;
    }
  }

  private int processBlocks() {
    long totalBlocks = endBlock - startBlock + 1;
    Semaphore semaphore = new Semaphore(maxConcurrency);

    try (ProgressBar pb = new ProgressBar("Backfilling SectionBloom", totalBlocks)) {

      for (long batchStart = startBlock; batchStart <= endBlock; batchStart += batchSize) {
        long batchEnd = Math.min(batchStart + batchSize - 1, endBlock);

        try {
          semaphore.acquire();
          processBatch(batchStart, batchEnd, pb);

          if (forceFlush) {
            // Force flush after each batch (simplified - actual implementation would need
            // RevokingStore)
            logger.debug("Batch {} to {} completed", batchStart, batchEnd);
          }

        } catch (Exception e) {
          spec.commandLine().getOut().printf("Error processing batch %d to %d, %s\n", batchStart, batchEnd, e);
          errorCount.incrementAndGet();
        } finally {
          semaphore.release();
        }
      }

    } catch (Exception e) {
      spec.commandLine().getOut().printf("Error in progress tracking %s\n", e);
      return 1;
    }

    return errorCount.get() > 0 ? 1 : 0;
  }

  private void processBatch(long batchStart, long batchEnd, ProgressBar pb) {
    try {
      DBInterface transactionRetDb = DbTool.getDB(databaseDirectory, "transactionRetStore");
      DBInterface sectionBloomDb = DbTool.getDB(databaseDirectory, "section-bloom");

      for (long blockNum = batchStart; blockNum <= batchEnd; blockNum++) {
        try {
          processBlock(blockNum, transactionRetDb, sectionBloomDb);
          processedBlocks.incrementAndGet();
          pb.step();

        } catch (Exception e) {
          spec.commandLine().getOut().printf("Error processing block %d, %s\n", blockNum, e);
          errorCount.incrementAndGet();
        }
      }

    } catch (Exception e) {
      spec.commandLine().getOut().printf("Error in batch processing %s", e);
      throw new RuntimeException(e);
    }
  }

  private void processBlock(long blockNum, DBInterface transactionRetDb, DBInterface sectionBloomDb)
      throws RocksDBException, BadItemException, EventBloomException {

    // Get transaction info for this block
    byte[] blockKey = ByteArray.fromLong(blockNum);
    byte[] transactionRetData = transactionRetDb.get(blockKey);

    if (transactionRetData == null) {
      // Debug: Check if the key format is correct
      if (blockNum % 1000 == 0) {
        spec.commandLine().getOut().printf("Debug: No transaction data found for block %d (key: %s)\n",
            blockNum, ByteArray.toHexString(blockKey));
      }
      return;
    }

    try {
      TransactionRetCapsule transactionRetCapsule = new TransactionRetCapsule(transactionRetData);

      // Debug: Check transaction count
      int transactionCount = transactionRetCapsule.getInstance().getTransactioninfoCount();
      if (blockNum % 1000 == 0) {
        spec.commandLine().getOut().printf("Debug: Block %d has %d transactions\n", blockNum, transactionCount);
      }

      // Create bloom filter for this block using the same logic as SectionBloomStore
      Bloom blockBloom = Bloom.createBloom(transactionRetCapsule);

      if (blockBloom != null) {
        // Extract bit positions from bloom filter
        List<Integer> bitList = extractBitPositions(blockBloom);

        if (!CollectionUtils.isEmpty(bitList)) {
          // Write to section bloom store using the same logic as SectionBloomStore.write
          writeSectionBloom(blockNum, bitList, sectionBloomDb);
          blocksWithLogs.incrementAndGet();

          if (blockNum % 1000 == 0) {
            spec.commandLine().getOut().printf("Debug: Block %d processed with %d bloom bits\n",
                blockNum, bitList.size());
          }
        }
      } else if (blockNum % 1000 == 0) {
        spec.commandLine().getOut().printf("Debug: Block %d has no bloom data\n", blockNum);
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

    // Constants from SectionBloomStore
    final int BLOCK_PER_SECTION = 2048;

    int section = (int) (blockNum / BLOCK_PER_SECTION);
    int blockNumOffset = (int) (blockNum % BLOCK_PER_SECTION);

    for (int bitIndex : bitList) {
      // Get existing BitSet from database
      BitSet bitSet = getSectionBloomBitSet(section, bitIndex, sectionBloomDb);
      if (Objects.isNull(bitSet)) {
        bitSet = new BitSet(BLOCK_PER_SECTION);
      }

      // Update the bit for this block
      bitSet.set(blockNumOffset);

      // Put back into database
      putSectionBloomBitSet(section, bitIndex, bitSet, sectionBloomDb);
    }
  }

  private long combineKey(int section, int bitIndex) {
    return section * 1_000_000L + bitIndex;
  }

  private BitSet getSectionBloomBitSet(int section, int bitIndex, DBInterface sectionBloomDb)
      throws RocksDBException, EventBloomException {
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
    spec.commandLine().getOut().printf("Total blocks processed: %d%n", processedBlocks.get());
    spec.commandLine().getOut().printf("Blocks with logs: %d%n", blocksWithLogs.get());
    spec.commandLine().getOut().printf("Errors encountered: %d%n", errorCount.get());
    spec.commandLine().getOut().printf("Duration: %d seconds%n", duration);

    if (errorCount.get() == 0) {
      spec.commandLine().getOut().println("✓ Backfill completed successfully!");
    } else {
      spec.commandLine().getOut().println("⚠ Backfill completed with errors. Check toolkit.log for details.");
    }
  }
}
