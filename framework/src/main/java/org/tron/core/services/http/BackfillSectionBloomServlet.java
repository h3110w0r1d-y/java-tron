package org.tron.core.services.http;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.tron.common.bloom.Bloom;
import org.tron.core.ChainBaseManager;
import org.tron.core.capsule.BlockCapsule;
import org.tron.core.capsule.TransactionRetCapsule;
import org.tron.core.exception.EventBloomException;
import org.tron.core.exception.StoreException;
import org.tron.core.store.SectionBloomStore;

@Component
@Slf4j(topic = "API")
public class BackfillSectionBloomServlet extends RateLimiterServlet {

  @Autowired
  private ChainBaseManager chainBaseManager;

  private static final ExecutorService backfillExecutor = Executors.newSingleThreadExecutor(
      r -> new Thread(r, "section-bloom-backfill"));

  // 用于控制并发的标志，确保同时只有一个backfill请求在处理
  private static final AtomicBoolean isProcessing = new AtomicBoolean(false);

  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response) {
    try {
      // 检查是否是状态查询请求
      String action = request.getParameter("action");
      if ("status".equals(action)) {
        handleStatusRequest(response);
        return;
      }

      long startBlock = Long.parseLong(request.getParameter("startBlock"));
      long endBlock = Long.parseLong(request.getParameter("endBlock"));

      handleBackfillRequest(startBlock, endBlock, response);
    } catch (NumberFormatException e) {
      logger.error("Invalid block number parameters", e);
      try {
        response.getWriter().println(Util.printErrorMsg(
            new IllegalArgumentException("Invalid block number parameters")));
      } catch (IOException ioe) {
        logger.debug("IOException: {}", ioe.getMessage());
      }
    } catch (Exception e) {
      Util.processError(e, response);
    }
  }

  @Override
  protected void doPost(HttpServletRequest request, HttpServletResponse response) {
    try {
      PostParams params = PostParams.getPostParams(request);
      JsonObject jsonObject = JsonParser.parseString(params.getParams()).getAsJsonObject();

      long startBlock = jsonObject.get("startBlock").getAsLong();
      long endBlock = jsonObject.get("endBlock").getAsLong();

      handleBackfillRequest(startBlock, endBlock, response);
    } catch (Exception e) {
      Util.processError(e, response);
    }
  }

  private void handleStatusRequest(HttpServletResponse response) throws IOException {
    JsonObject statusResult = new JsonObject();
    boolean currentlyProcessing = isProcessing.get();

    statusResult.addProperty("isProcessing", currentlyProcessing);
    statusResult.addProperty("status", currentlyProcessing ? "busy" : "idle");
    statusResult.addProperty("message", currentlyProcessing ? "A backfill operation is currently in progress"
        : "No backfill operation is currently running");

    response.getWriter().println(statusResult.toString());
    logger.debug("Status request handled: processing={}", currentlyProcessing);
  }

  private void handleBackfillRequest(long startBlock, long endBlock, HttpServletResponse response)
      throws IOException {
    // 检查是否有其他backfill请求正在处理
    if (!isProcessing.compareAndSet(false, true)) {
      logger.warn("Another backfill request is already in progress. Rejecting new request for blocks {} to {}",
          startBlock, endBlock);
      JsonObject errorResult = new JsonObject();
      errorResult.addProperty("status", "rejected");
      errorResult.addProperty("error", "Another backfill request is already in progress");
      errorResult.addProperty("message",
          "Please wait for the current backfill operation to complete before starting a new one");
      response.getWriter().println(errorResult.toString());
      return;
    }

    try {
      // 参数验证
      if (startBlock < 0 || endBlock < 0) {
        logger.warn("Invalid block numbers: startBlock={}, endBlock={}", startBlock, endBlock);
        response.getWriter().println(Util.printErrorMsg(
            new IllegalArgumentException("Block numbers must be non-negative")));
        return;
      }

      if (startBlock > endBlock) {
        logger.warn("Invalid block range: startBlock={} > endBlock={}", startBlock, endBlock);
        response.getWriter().println(Util.printErrorMsg(
            new IllegalArgumentException("Start block must be less than or equal to end block")));
        return;
      }

      long currentHeadBlock = chainBaseManager.getHeadBlockNum();
      if (startBlock > currentHeadBlock || endBlock > currentHeadBlock) {
        logger.warn("Block numbers exceed current head block: startBlock={}, endBlock={}, headBlock={}",
            startBlock, endBlock, currentHeadBlock);
        response.getWriter().println(Util.printErrorMsg(
            new IllegalArgumentException("Block numbers cannot exceed current head block: " + currentHeadBlock)));
        return;
      }

      // 检查当前是否启用了JsonRpcFilter
      boolean isJsonRpcFilterEnabled = org.tron.common.parameter.CommonParameter.getInstance().isJsonRpcFilterEnabled();
      logger.info("Starting SectionBloom backfill for blocks {} to {}, JsonRpcFilter enabled: {}",
          startBlock, endBlock, isJsonRpcFilterEnabled);

      if (!isJsonRpcFilterEnabled) {
        logger.warn("JsonRpcFilter is not currently enabled. Consider enabling it before backfilling.");
      }

      // 异步处理，避免阻塞HTTP请求
      CompletableFuture<String> backfillTask = CompletableFuture.supplyAsync(() -> {
        try {
          return performBackfill(startBlock, endBlock);
        } catch (Exception e) {
          logger.error("Error during SectionBloom backfill", e);
          return "Error: " + e.getMessage();
        } finally {
          // 在异步任务完成后释放锁
          isProcessing.set(false);
          logger.info("Backfill processing lock released");
        }
      }, backfillExecutor);

      try {
        // 等待一小段时间看是否能快速完成
        String result = backfillTask.get(java.util.concurrent.TimeUnit.SECONDS.toMillis(5),
                java.util.concurrent.TimeUnit.MILLISECONDS);
        response.getWriter().println(result);
      } catch (java.util.concurrent.TimeoutException e) {
        // 如果超时，返回异步处理状态
        JsonObject result = new JsonObject();
        result.addProperty("status", "processing");
        result.addProperty("message", "SectionBloom backfill started for blocks " + startBlock + " to " + endBlock);
        result.addProperty("startBlock", startBlock);
        result.addProperty("endBlock", endBlock);
        response.getWriter().println(result.toString());
      } catch (Exception e) {
        logger.error("Error waiting for backfill result", e);
        response.getWriter().println(Util.printErrorMsg(e));
        // 如果在等待过程中出错，也需要释放锁
        isProcessing.set(false);
      }
    } catch (Exception e) {
      // 如果在参数验证阶段出错，释放锁
      isProcessing.set(false);
      throw e;
    }
  }

  private String performBackfill(long startBlock, long endBlock) throws Exception {
    SectionBloomStore sectionBloomStore = chainBaseManager.getSectionBloomStore();
    long processedBlocks = 0;
    long skippedBlocks = 0;
    long errorBlocks = 0;
    long totalBlocks = endBlock - startBlock + 1;

    logger.info("Starting backfill process for {} blocks (from {} to {})", totalBlocks, startBlock, endBlock);

    for (long blockNum = startBlock; blockNum <= endBlock; blockNum++) {
      try {
        // 获取区块数据
        BlockCapsule blockCapsule = chainBaseManager.getBlockByNum(blockNum);
        if (blockCapsule == null) {
          logger.warn("Block {} not found, skipping", blockNum);
          skippedBlocks++;
          continue;
        }

        // 获取该区块的交易信息
        TransactionRetCapsule transactionRetCapsule = chainBaseManager.getTransactionRetStore()
            .getTransactionInfoByBlockNum(org.tron.common.utils.ByteArray.fromLong(blockNum));

        if (transactionRetCapsule == null) {
          logger.debug("No transaction info found for block {}, skipping", blockNum);
          skippedBlocks++;
          continue;
        }

        // 处理SectionBloom - 模拟Manager.java中的逻辑
        Bloom blockBloom = sectionBloomStore.initBlockSection(transactionRetCapsule);
        if (blockBloom != null) {
          sectionBloomStore.write(blockNum);
          processedBlocks++;

          if (processedBlocks % 100 == 0) {
            long progress = ((blockNum - startBlock + 1) * 100) / totalBlocks;
            logger.info("Progress: {}% - Processed {} blocks, current block: {}", progress, processedBlocks, blockNum);
          }
        } else {
          // 即使没有bloom数据，也要调用write方法来确保数据一致性
          sectionBloomStore.write(blockNum);
          skippedBlocks++;
        }

      } catch (StoreException e) {
        logger.warn("Store exception for block {}: {}", blockNum, e.getMessage());
        errorBlocks++;
      } catch (EventBloomException e) {
        logger.warn("EventBloom exception for block {}: {}", blockNum, e.getMessage());
        errorBlocks++;
      } catch (Exception e) {
        logger.error("Unexpected error processing block {}", blockNum, e);
        errorBlocks++;
      }
    }

    // 构建结果
    JsonObject result = new JsonObject();
    result.addProperty("status", "completed");
    result.addProperty("startBlock", startBlock);
    result.addProperty("endBlock", endBlock);
    result.addProperty("processedBlocks", processedBlocks);
    result.addProperty("skippedBlocks", skippedBlocks);
    result.addProperty("errorBlocks", errorBlocks);
    result.addProperty("totalBlocks", endBlock - startBlock + 1);

    logger.info("SectionBloom backfill completed for blocks {} to {}. " +
        "Total: {}, Processed: {}, Skipped: {}, Errors: {}, Success rate: {:.2f}%",
        startBlock, endBlock, totalBlocks, processedBlocks, skippedBlocks, errorBlocks,
        (processedBlocks * 100.0) / totalBlocks);

    return result.toString();
  }
}
