# Toolkit Manual

This package contains a set of tools for TRON, the followings are the documentation for each tool.

## DB Archive

DB archive provides the ability to reformat the manifest according to the current `database`, parameters are compatible with the previous `ArchiveManifest`.

### Available parameters:

- `-b | --batch-size`: Specify the batch manifest size, default: 80000.
- `-d | --database-directory`: Specify the database directory to be processed, default: output-directory/database.
- `-m | --manifest-size`: Specify the minimum required manifest file size, unit: M, default: 0.
- `-h | --help`: Provide the help info.

### Examples:

```shell script
# full command
  java -jar Toolkit.jar db archive [-h] [-b=<maxBatchSize>] [-d=<databaseDirectory>] [-m=<maxManifestSize>]
# examples
   java -jar Toolkit.jar db archive #1. use default settings
   java -jar Toolkit.jar db archive -d /tmp/db/database #2. specify the database directory as /tmp/db/database
   java -jar Toolkit.jar db archive -b 64000 #3. specify the batch size to 64000 when optimizing manifest
   java -jar Toolkit.jar db archive -m 128 #4. specify optimization only when Manifest exceeds 128M
```


## DB Convert

DB convert provides a helper which can convert LevelDB data to RocksDB data, parameters are compatible with previous `DBConvert`.

### Available parameters:

- `<src>`: Input path for leveldb, default: output-directory/database.
- `<dest>`: Output path for rocksdb, default: output-directory-dst/database.
- `--safe`: In safe mode, read data from leveldb then put into rocksdb, it's a very time-consuming procedure. If not, just change engine.properties from leveldb to rocksdb, rocksdb
  is compatible with leveldb for the current version. This may not be the case in the future, default: false.
- `-h | --help`: Provide the help info.

### Examples:

```shell script
# full command
  java -jar Toolkit.jar db convert [-h] [--safe] <src> <dest>
# examples
  java -jar Toolkit.jar db convert  output-directory/database /tmp/database
```

## DB Copy

DB copy provides a helper which can copy LevelDB or RocksDB data quickly on the same file systems by creating hard links.

### Available parameters:

- `<src>`: Source path for database. Default: output-directory/database
- `<dest>`: Output path for database. Default: output-directory-cp/database
- `-h | --help`: provide the help info

### Examples:

```shell script
# full command
  java -jar Toolkit.jar db cp [-h] <src> <dest>
# examples
  java -jar Toolkit.jar db cp  output-directory/database /tmp/databse
```

## DB Lite

DB lite provides lite database, parameters are compatible with previous `LiteFullNodeTool`.

### Available parameters:

- `-o | --operate`: [split,merge], default: split.
- `-t | --type`: Only used with operate=split: [snapshot,history], default: snapshot.
- `-fn | --fn-data-path`: The database path to be split or merged.
- `-ds | --dataset-path`: When operation is `split`,`dataset-path` is the path that store the `snapshot` or `history`, when
  operation is `split`, `dataset-path` is the `history` data path.
- `-h | --help`: Provide the help info.

### Examples:

```shell script
# full command
  java -jar Toolkit.jar db lite [-h] -ds=<datasetPath> -fn=<fnDataPath> [-o=<operate>] [-t=<type>]
# examples
  #split and get a snapshot dataset
  java -jar Toolkit.jar db lite -o split -t snapshot --fn-data-path output-directory/database --dataset-path /tmp
  #split and get a history dataset
  java -jar Toolkit.jar db lite -o split -t history --fn-data-path output-directory/database --dataset-path /tmp
  #merge history dataset and snapshot dataset
  java -jar Toolkit.jar db lite -o merge --fn-data-path /tmp/snapshot --dataset-path /tmp/history
```

## DB Backfill Bloom

DB backfill bloom provides the ability to backfill SectionBloom data for historical blocks to enable eth_getLogs address filtering. This is useful when `isJsonRpcFilterEnabled` was disabled during block processing and later enabled, causing historical blocks to lack SectionBloom data.

### Available parameters:

- `-d | --database-directory`: Specify the database directory path, default: output-directory/database.
- `-s | --start-block`: Specify the start block number for backfill (required).
- `-e | --end-block`: Specify the end block number for backfill (optional, default: latest block).
- ~~`-b | --batch-size`~~: Batch size is fixed at 2048 blocks (one section) for optimal performance.
- `-c | --max-concurrency`: Specify the maximum concurrency for processing, default: 5.
- `-f | --force-flush`: Force database flush after each batch, default: true.
- `-h | --help`: Provide the help info.

### Examples:

```shell script
# full command
  java -jar Toolkit.jar db backfill-bloom [-h] -s=<startBlock> [-e=<endBlock>] [-d=<databaseDirectory>] [-c=<maxConcurrency>] [-f=<forceFlush>]
# examples
   java -jar Toolkit.jar db backfill-bloom -s 1000000 -e 2000000 #1. backfill blocks 1000000 to 2000000
   java -jar Toolkit.jar db backfill-bloom -s 1000000 -d /path/to/database #2. specify custom database directory
   java -jar Toolkit.jar db backfill-bloom -s 1000000 -c 8 #3. use higher concurrency (8 threads)
   java -jar Toolkit.jar db backfill-bloom -s 1000000 --force-flush=false #4. disable force flush for better performance

### Progress Monitoring:

The progress bar shows the total number of blocks scanned (regardless of success/failure). Additional progress information is displayed every 1000 blocks:

```
Progress: 5000 blocks scanned, 4950 successful, 1200 with logs, 50 errors
```

This means:
- **5000 blocks scanned**: Total blocks examined (matches progress bar)
- **4950 successful**: Blocks processed without errors
- **1200 with logs**: Blocks that contained transaction logs and had SectionBloom data written
- **50 errors**: Blocks that failed to process (database errors, corruption, etc.)

### Performance Considerations:

1. **Section-Based Multi-threading**: Each thread processes exactly one section (2048 blocks) aligned to section boundaries. This eliminates lock contention since different threads never modify the same bloom section.

   **Example**: For blocks 1000-4000:
   - Thread 1 processes Section 0: blocks [1000-2047]
   - Thread 2 processes Section 1: blocks [2048-4000]

2. **Lock-Free Design**: No synchronization overhead for bloom writes. Each thread operates on independent data structures.

3. **Optimal Batch Size**: Fixed at 2048 blocks (one section) for maximum efficiency. This aligns perfectly with SectionBloom's internal structure.

4. **Thread-Safe Progress**: Progress bar and console output are synchronized to prevent garbled display. You'll see thread names in progress messages.

5. **Force Flush**: Enabling force flush ensures data persistence but reduces performance. Disable for faster processing if system stability is guaranteed.

### Recommended Settings:

```bash
# High-performance setup (fast SSD, lots of RAM)
java -jar Toolkit.jar db backfill-bloom -s 1000000 -e 2000000 -c 8 --force-flush=false

# Balanced setup (most systems)
java -jar Toolkit.jar db backfill-bloom -s 1000000 -e 2000000 -c 5 --force-flush=true

# Conservative setup (slower systems, network storage)
java -jar Toolkit.jar db backfill-bloom -s 1000000 -e 2000000 -c 2 --force-flush=true
```

### Architecture Benefits:

- **No Lock Contention**: Each thread processes different sections, eliminating synchronization overhead
- **Perfect Load Distribution**: Work is evenly divided into 2048-block sections
- **Memory Efficiency**: Each thread maintains minimal state, no shared data structures
- **Scalability**: Performance scales linearly with CPU cores (up to I/O limits)

## DB Debug Tool

DB debug tool provides debugging capabilities for database connections and data verification, particularly useful for troubleshooting backfill operations.

### Available parameters:

- `-d | --database-directory`: Specify the database directory path, default: output-directory/database.
- `-s | --start-block`: Specify the start block number to check, default: 1000000.
- `-e | --end-block`: Specify the end block number to check, default: 1000010.
- `-h | --help`: Provide the help info.

### Examples:

```shell script
# full command
  java -jar Toolkit.jar db debug-db [-h] [-d=<databaseDirectory>] [-s=<startBlock>] [-e=<endBlock>]
# examples
   java -jar Toolkit.jar db debug-db #1. use default settings
   java -jar Toolkit.jar db debug-db -d /path/to/database #2. specify custom database directory
   java -jar Toolkit.jar db debug-db -s 2000000 -e 2000100 #3. check specific block range
```

## DB Move

DB move provides a helper to move some dbs to a pre-set new path. For example move `block`, `transactionRetStore` or `transactionHistoryStore` to HDD for reducing storage expenses.

### Available parameters:

- `-c | --config`: config file. Default: config.conf.
- `-d | --database-directory`: database directory path. Default: output-directory.
- `-h | --help`: provide the help info

### Examples:

Take the example of moving `block` and `trans`.


Set path for `block` and `trans`.

```conf
storage {
 ......
  properties = [
    {
     name = "block",
     path = "/data1/tron",
    },
    {
     name = "trans",
     path = "/data1/tron",
   }
  ]
 ......
}
```
Execute move command.
```shell script
# full command
  java -jar Toolkit.jar db mv [-h] [-c=<config>] [-d=<database>]
# examples
  java -jar Toolkit.jar db mv -c main_net_config.conf -d /data/tron/output-directory
```

## DB Root

DB root provides a helper which can compute merkle root for tiny db.

NOTE: large db may GC overhead limit exceeded.

### Available parameters:

- `<src>`: Source path for database. Default: output-directory/database
- `--db`: db name.
- `-h | --help`: provide the help info
