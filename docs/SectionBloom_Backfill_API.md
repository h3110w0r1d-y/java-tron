# SectionBloom Backfill API

## 概述

SectionBloom Backfill API 是为 java-tron 添加的一个 HTTP API，用于补全指定区块范围内未处理的 SectionBloom 数据。

当 `isJsonRpcFilterEnabled` 配置未启用时，历史区块的交易数据不会被处理为 SectionBloom。这个 API 允许管理员在后续启用该功能后，回填历史区块的 SectionBloom 数据。

## API 端点

```
POST/GET /wallet/backfillsectionbloom
```

### 状态查询端点

```
GET /wallet/backfillsectionbloom?action=status
```

## 请求参数

### GET 请求
- `startBlock` (必需): 起始区块号
- `endBlock` (必需): 结束区块号

示例:
```
GET /wallet/backfillsectionbloom?startBlock=1000&endBlock=2000
```

### POST 请求
请求体 (JSON):
```json
{
  "startBlock": 1000,
  "endBlock": 2000
}
```

## 参数限制

1. **区块号范围**: `startBlock` 和 `endBlock` 必须为非负数
2. **区块顺序**: `startBlock` 必须小于或等于 `endBlock`
3. **区块存在性**: 区块号不能超过当前链的头块号
4. **并发限制**: 同时只能有一个backfill请求在处理，第二个请求会被直接拒绝

## 响应格式

### 成功响应 (同步完成)
```json
{
  "status": "completed",
  "startBlock": 1000,
  "endBlock": 2000,
  "processedBlocks": 950,
  "skippedBlocks": 45,
  "errorBlocks": 5,
  "totalBlocks": 1001
}
```

### 异步处理响应
```json
{
  "status": "processing",
  "message": "SectionBloom backfill started for blocks 1000 to 2000",
  "startBlock": 1000,
  "endBlock": 2000
}
```

### 状态查询响应
```json
{
  "isProcessing": false,
  "status": "idle",
  "message": "No backfill operation is currently running"
}
```

或者当有操作正在进行时：
```json
{
  "isProcessing": true,
  "status": "busy",
  "message": "A backfill operation is currently in progress"
}
```

### 并发拒绝响应
```json
{
  "status": "rejected",
  "error": "Another backfill request is already in progress",
  "message": "Please wait for the current backfill operation to complete before starting a new one"
}
```

### 错误响应
```json
{
  "Error": "Block numbers must be non-negative"
}
```

## 响应字段说明

- `status`: 处理状态 (`completed` 或 `processing`)
- `startBlock`: 起始区块号
- `endBlock`: 结束区块号
- `processedBlocks`: 成功处理的区块数
- `skippedBlocks`: 跳过的区块数（无交易数据或无需处理）
- `errorBlocks`: 处理失败的区块数
- `totalBlocks`: 总区块数

## 使用场景

1. **初次启用 JsonRpcFilter**: 当首次启用 `isJsonRpcFilterEnabled` 配置后，需要回填历史数据
2. **数据修复**: 当发现某些区块的 SectionBloom 数据缺失或损坏时
3. **数据迁移**: 在节点迁移或数据恢复后补全 SectionBloom 数据

## 注意事项

1. **性能影响**: 大范围的回填操作可能会影响节点性能，建议在低峰期执行
2. **超时处理**: 大范围请求会异步处理，避免 HTTP 请求超时
3. **重复执行**: 可以安全地重复执行相同范围的回填操作
4. **配置检查**: API 会检查当前 JsonRpcFilter 是否启用，并在日志中记录警告
5. **并发限制**: 同时只能有一个backfill请求在处理，确保数据一致性和系统稳定性
6. **无范围限制**: 移除了区块范围限制，可以处理任意大小的区块范围

## 错误码

- `Block numbers must be non-negative`: 区块号为负数
- `Start block must be less than or equal to end block`: 区块范围无效
- `Block numbers cannot exceed current head block: X`: 区块号超出当前头块
- `Another backfill request is already in progress`: 已有其他backfill请求正在处理

## 日志记录

API 会在以下情况记录日志：
- 开始处理时记录起始信息
- 每处理 100 个区块记录进度
- 处理完成时记录统计信息
- 发生错误时记录错误详情

## 示例用法

### 使用 curl 命令

```bash
# 状态查询
curl "http://localhost:8090/wallet/backfillsectionbloom?action=status"

# GET 请求
curl "http://localhost:8090/wallet/backfillsectionbloom?startBlock=1000&endBlock=2000"

# POST 请求
curl -X POST http://localhost:8090/wallet/backfillsectionbloom \
  -H "Content-Type: application/json" \
  -d '{"startBlock": 1000, "endBlock": 2000}'
```

### 分批处理大范围数据

由于并发限制，需要等待前一个请求完成后再发起下一个请求：

```bash
# 处理 100,000 个区块，分成 10 批
for i in {0..9}; do
  start=$((i * 10000 + 1))
  end=$(((i + 1) * 10000))
  echo "Processing batch $((i+1))/10: blocks $start to $end"

  # 检查当前状态
  status=$(curl -s "http://localhost:8090/wallet/backfillsectionbloom?action=status")
  echo "Current status: $status"

  # 如果有操作正在进行，等待完成
  while echo "$status" | grep -q '"isProcessing":true'; do
    echo "Another operation is in progress, waiting..."
    sleep 30
    status=$(curl -s "http://localhost:8090/wallet/backfillsectionbloom?action=status")
  done

  # 发起请求
  response=$(curl -s "http://localhost:8090/wallet/backfillsectionbloom?startBlock=$start&endBlock=$end")
  echo "Response: $response"

  # 检查是否是异步处理
  if echo "$response" | grep -q '"status":"processing"'; then
    echo "Batch is processing asynchronously, monitoring status..."
    # 监控状态直到完成
    while true; do
      sleep 30
      status=$(curl -s "http://localhost:8090/wallet/backfillsectionbloom?action=status")
      if echo "$status" | grep -q '"isProcessing":false'; then
        echo "Batch $((i+1)) completed"
        break
      fi
      echo "Still processing..."
    done
  fi

  echo "Batch $((i+1)) finished, starting next batch..."
done
```

## 监控和故障排除

1. **检查日志**: 查看节点日志中的 SectionBloom backfill 相关信息
2. **监控性能**: 观察节点的 CPU 和 I/O 使用情况
3. **验证结果**: 可以通过 JsonRpc 查询验证 SectionBloom 数据是否正确生成

## 相关配置

确保以下配置正确设置：
```
# 启用 JsonRpc Filter
jsonRpcHttpFullNodeEnable = true
# 或
jsonRpcHttpSolidityNodeEnable = true
```
