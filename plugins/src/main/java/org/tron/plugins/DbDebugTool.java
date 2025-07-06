package org.tron.plugins;

import java.io.File;
import java.util.concurrent.Callable;
import lombok.extern.slf4j.Slf4j;
import org.tron.plugins.utils.ByteArray;
import org.tron.plugins.utils.db.DBInterface;
import org.tron.plugins.utils.db.DbTool;
import picocli.CommandLine;

@Slf4j(topic = "debug-tool")
@CommandLine.Command(name = "debug-db",
    description = "Debug database connections and data for backfill operations.",
    exitCodeListHeading = "Exit Codes:%n",
    exitCodeList = {
        "0:Successful",
        "1:Internal error: exception occurred, please check toolkit.log"})
public class DbDebugTool implements Callable<Integer> {

  @CommandLine.Spec
  CommandLine.Model.CommandSpec spec;

  @CommandLine.Option(
      names = {"--database-directory", "-d"},
      defaultValue = "output-directory/database",
      description = "Database directory path. Default: ${DEFAULT-VALUE}",
      order = 1)
  private String databaseDirectory;

  @CommandLine.Option(
      names = {"--start-block", "-s"},
      defaultValue = "1000000",
      description = "Start block number to check",
      order = 2)
  private long startBlock;

  @CommandLine.Option(
      names = {"--end-block", "-e"},
      defaultValue = "1000010",
      description = "End block number to check",
      order = 3)
  private long endBlock;

  @CommandLine.Option(
      names = {"--help", "-h"},
      help = true,
      description = "Display help message",
      order = 4)
  private boolean help;

  @Override
  public Integer call() {
    if (help) {
      spec.commandLine().usage(System.out);
      return 0;
    }

    try {
      // Check database directory
      File dbDir = new File(databaseDirectory);
      if (!dbDir.exists() || !dbDir.isDirectory()) {
        spec.commandLine().getErr().println("Database directory does not exist: " + databaseDirectory);
        return 1;
      }

      spec.commandLine().getOut().println("Database directory: " + databaseDirectory);
      
      // List all databases in the directory
      File[] subdirs = dbDir.listFiles(File::isDirectory);
      if (subdirs != null) {
        spec.commandLine().getOut().println("Available databases:");
        for (File subdir : subdirs) {
          spec.commandLine().getOut().println("  - " + subdir.getName());
        }
      }

      // Test TransactionRetStore connection
      spec.commandLine().getOut().println("\n=== Testing TransactionRetStore ===");
      try {
        DBInterface transactionRetDb = DbTool.getDB(databaseDirectory, "transactionRetStore");
        spec.commandLine().getOut().println("✓ TransactionRetStore connection successful");
        
        // Test reading some blocks
        int foundBlocks = 0;
        for (long blockNum = startBlock; blockNum <= endBlock; blockNum++) {
          byte[] blockKey = ByteArray.fromLong(blockNum);
          byte[] data = transactionRetDb.get(blockKey);
          if (data != null) {
            foundBlocks++;
            spec.commandLine().getOut().printf("  Block %d: found data (%d bytes)\n", blockNum, data.length);
          } else {
            spec.commandLine().getOut().printf("  Block %d: no data\n", blockNum);
          }
        }
        spec.commandLine().getOut().printf("Found data for %d out of %d blocks\n", foundBlocks, (endBlock - startBlock + 1));
        
      } catch (Exception e) {
        spec.commandLine().getErr().println("✗ TransactionRetStore connection failed: " + e.getMessage());
      }

      // Test SectionBloom connection
      spec.commandLine().getOut().println("\n=== Testing SectionBloom ===");
      try {
        DBInterface sectionBloomDb = DbTool.getDB(databaseDirectory, "section-bloom");
        spec.commandLine().getOut().println("✓ SectionBloom connection successful");
        
        // Test reading some section bloom data
        int section = (int) (startBlock / 2048);
        for (int bitIndex = 0; bitIndex < 10; bitIndex++) {
          long keyLong = section * 1_000_000L + bitIndex;
          byte[] key = Long.toHexString(keyLong).getBytes();
          byte[] data = sectionBloomDb.get(key);
          if (data != null) {
            spec.commandLine().getOut().printf("  Section %d, bit %d: found data (%d bytes)\n", 
                section, bitIndex, data.length);
          }
        }
        
      } catch (Exception e) {
        spec.commandLine().getErr().println("✗ SectionBloom connection failed: " + e.getMessage());
      }

      return 0;

    } catch (Exception e) {
      logger.error("Debug failed", e);
      spec.commandLine().getErr().println("Debug failed: " + e.getMessage());
      return 1;
    } finally {
      DbTool.close();
    }
  }
}
