# Velox IO 模型详解：从两级缓存到格式层并发

本文件从两个维度剖析 Velox 的 IO 栈：**(1)** 以 `AsyncDataCache` + `SsdCache` 为核心的两层级联缓存机制（含 IO 合并、预读、并发与反压）；**(2)** 上层格式读取器（Parquet / DWRF 等）向下层提交 IO 的并发模型与各格式特有的读取流程。所有代码引用均带 `文件:行号` 锚点，便于跳转核对。

---

## 目录

- [第一部分：内存-SSD 两层级联缓存](#第一部分内存-ssd-两层级联缓存)
  - [1.1 总体架构与数据通路](#11-总体架构与数据通路)
  - [1.2 内存层：AsyncDataCache](#12-内存层asyncdatacache)
    - [1.2.4 内存容量与分配策略](#124-内存容量与分配策略)
    - [1.2.5 驱逐策略：clock + 动态百分位阈值](#125-驱逐策略clock--动态百分位阈值)
    - [1.2.6 驱逐失败与内存压力应对](#126-驱逐失败与内存压力应对)
  - [1.3 SSD 层：SsdCache / SsdFile](#13-ssd-层ssdcache--ssdfile)
    - [1.3.4 SSD 容量与文件增长](#134-ssd-容量与文件增长)
    - [1.3.5 SSD 写入触发链（从 entry 到 batch）](#135-ssd-写入触发链从-entry-到-batch)
    - [1.3.6 SSD 侧淘汰：region-based + score-based](#136-ssd-侧淘汰region-based--score-based)
    - [1.3.7 Checkpoint 与重启恢复](#137-checkpoint-与重启恢复)
  - [1.4 级联查找：Memory → SSD → Storage](#14-级联查找memory--ssd--storage)
  - [1.5 IO 合并：CoalesceIo](#15-io-合并coalesceio)
  - [1.6 预读 / Readahead / Preload](#16-预读--readahead--preload)
  - [1.7 并发、Sharding 与反压](#17-并发sharding-与反压)
  - [1.8 其他缓存机制：TTL / FileGroupStats / 统计 / 文件注册](#18-其他缓存机制ttl--filegroupstats--统计--文件注册)
- [第二部分：上层格式读取的 IO 并发](#第二部分上层格式读取的-io-并发)
  - [2.1 抽象层次总览](#21-抽象层次总览)
  - [2.2 Reader / RowReader / ColumnReader 三层体系](#22-reader--rowreader--columnreader-三层体系)
  - [2.3 BufferedInput 与 CachedBufferedInput](#23-bufferedinput-与-cachedbufferedinput)
  - [2.4 Parquet：Row Group 调度与列并发](#24-parquetrow-group-调度与列并发)
  - [2.5 DWRF/ORC：Stripe 与 UnitLoader](#25-dwrforcstripe-与-unitloader)
  - [2.6 Scan 算子到 Reader 的协同](#26-scan-算子到-reader-的协同)
  - [2.7 完整 IO 提交链路](#27-完整-io-提交链路)

---

# 第一部分：内存-SSD 两层级联缓存

## 1.1 总体架构与数据通路

Velox 的缓存栈是一个"三级存储"结构：**内存缓存 → SSD 缓存 → 远程/本地文件系统**。一次 IO 命中时直接返回，未命中时根据策略向下穿透；热点数据在被驱逐出内存前会被异步写入 SSD，下次命中时从 SSD 读回内存。

```
                   ┌─────────────────────────────────────────────┐
   上层 Reader      │  CachedBufferedInput  (请求提交入口)         │
   (Parquet/DWRF)  │  - enqueue(region)                          │
                   │  - load()  / prefetch()                     │
                   └───────────────┬─────────────────────────────┘
                                   │  CacheRequest{key, offset, size}
                                   ▼
            ┌──────────────────────────────────────────────────┐
            │           内存层  AsyncDataCache                  │
            │  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐ │
            │  │Shard 0  │ │Shard 1  │ │Shard 2  │ │Shard 3  │ │
            │  │Entry Map│ │Entry Map│ │Entry Map│ │Entry Map│ │
            │  └─────────┘ └─────────┘ └─────────┘ └─────────┘ │
            │           (命中即返回 CachePin)                    │
            └───────────────┬──────────────────┬────────────────┘
                       miss │                  │ eviction 时异步
                            ▼                  ▼ saveable
            ┌──────────────────────────────────────────────────┐
            │           SSD 层  SsdCache (可选)                 │
            │   File 0..N (各 64MB region，按 fileId hash 分片) │
            │   命中返回 SsdPin，未命中穿透到底层                │
            └───────────────┬──────────────────────────────────┘
                            │ miss
                            ▼
            ┌──────────────────────────────────────────────────┐
            │  ReadFileInputStream (封装 ReadFile，如 S3/HDFS)  │
            │   read() / readAsync() / vread()                  │
            └──────────────────────────────────────────────────┘
```

**关键设计**：
- **内存缓存是主体**：SSD 是附属层，只在内存吃紧时才发挥价值。`SsdCache` 通过 `std::unique_ptr<SsdCache> ssdCache_` 持有，可以为 `nullptr`（见 `velox/common/caching/AsyncDataCache.h:1033`）。
- **Sharded 设计降低锁争用**：内存缓存按 `key.fileId % numShards` 哈希到固定 shard，每个 shard 有独立 mutex 与 eviction clock。
- **SSD 异步写入**：evict 时如果 entry 标记了 `ssdSaveable_`，不直接丢弃，而是通过 executor 异步写到 SSD。

---

## 1.2 内存层：AsyncDataCache

### 1.2.1 核心类与 Options

定义在 `velox/common/caching/AsyncDataCache.h:798`：

```cpp
class AsyncDataCache : public memory::Cache {
 public:
  static constexpr int32_t kDefaultNumShards = 4;

  struct Options {
    double maxWriteRatio;          // 写入 SSD 的 entry 占比上限，默认 0.7
    double ssdSavableRatio;        // 触发 SSD 写入的最低内存占比，默认 0.125
    int32_t minSsdSavableBytes;    // 触发 SSD 写入的最低绝对值，默认 16MB
    int32_t numShards;             // 必须为 2 的幂
    uint64_t ssdFlushThresholdBytes; // 触发批量 flush 的阈值
  };
```

Options 的含义在 `AsyncDataCache.h:802-842` 有完整注释。`maxWriteRatio` 控制 SSD 写入并发度（避免写爆），`ssdSavableRatio` + `minSsdSavableBytes` 是触发写 SSD 的双阈值。

### 1.2.2 CacheEntry：原子 Pin 状态机

`AsyncDataCacheEntry`（`AsyncDataCache.h:156`）是缓存条目。它的 `numPins_` 是整个并发模型的核心：

```cpp
class AsyncDataCacheEntry {
  static constexpr int32_t kExclusive = -10000;  // 独占写入态
  static constexpr int32_t kTinyDataSize = 2048; // 小数据优化阈值

  // 三种存储形态（按 size 自动选择）
  memory::Allocation nonContiguousData_;  // 非连续页（大块）
  void* contiguousData_{nullptr};         // 连续内存（中等）
  std::string tinyData_;                  // 小于 2KB 直接用 std::string

  std::atomic<int32_t> numPins_{0};       // >0 共享读者数；=0 可驱逐；=kExclusive 独占写
  AccessStats accessStats_;               // 驱逐打分用
  tsan_atomic<SsdFile*> ssdFile_{nullptr};
  tsan_atomic<uint64_t> ssdOffset_{0};
  std::atomic<bool> ssdSaveable_{false};
};
```

**Pin 状态的三态转换**：

```
   ┌─────────────┐  setExclusiveToShared()  ┌──────────────┐
   │  kExclusive │ ───────────────────────► │  numPins_==1 │
   │  (= -10000) │   (写入完成，转共享)       │  (可读)       │
   └─────────────┘                          └──────┬───────┘
        ▲                                           │
        │ allocateExclusive()                       │ addReference() / release()
        │                                           ▼
   ┌─────────────┐  release() (最后一个)     ┌──────────────┐
   │   (alloc)   │ ◄─────────────────────── │  numPins_>1  │
   └─────────────┘                          │  (并发读)      │
                                            └──────────────┘
                                                    │ release() 后 numPins_==0
                                                    ▼
                                            ┌──────────────┐
                                            │  unpinned     │
                                            │  (可被 evict) │
                                            └──────────────┘
```

**关键操作**（`velox/common/caching/AsyncDataCache.cpp:76-132`）：

```cpp
void AsyncDataCacheEntry::setExclusiveToShared(bool ssdSavable) {
  VELOX_CHECK(isExclusive());
  numPins_ = 1;
  // 满足条件时标记为 SSD 可保存，evict 时会异步写到 SSD
  if (ssdCache != nullptr && ssdFile_ == nullptr) {
    if (ssdCache->groupStats().shouldSaveToSsd(groupId_, trackingId_)) {
      ssdSaveable_ = true;
      shard_->cache()->possibleSsdSave(size_);
    }
  }
}

void AsyncDataCacheEntry::release() {
  VELOX_CHECK_NE(0, numPins_);
  if (numPins_ == kExclusive) {   // 独占态释放 = entry 被废弃
    auto promise = shard_->removeEntry(this);
    if (promise != nullptr) promise->setValue(true);
    numPins_ = 0;
  } else {
    const auto oldPins = numPins_.fetch_add(-1);
    VELOX_CHECK_LE(1, oldPins, "pin count goes negative");
  }
}
```

### 1.2.3 CacheShard：分片降低锁争用

每个 shard 是独立的子缓存（`AsyncDataCache.h` 中 `CacheShard` 类，约 `:600-796`）。关键成员：

- `entries_`：`F14HashMAP` 从 `FileCacheKey` 到 `AsyncDataCacheEntry*`
- `clockHand_`：clock 算法扫描位置
- `evictionThreshold_`：动态校准的驱逐阈值

**驱逐算法**（`AsyncDataCache.cpp:515-645` 的 `CacheShard::evict`）：

```cpp
uint64_t CacheShard::evict(
    uint64_t bytesToFree, bool evictAllUnpinned,
    uint64_t bytesToAcquire, AcquiredMemory& acquired) {
  // clock 扫描 + 动态阈值
  auto entryIndex = (clockHand_ % size);
  int32_t score = candidate->score(now);
  if (candidate->numPins_ == 0 &&
      (evictAllUnpinned || score >= evictionThreshold_)) {
    // SSD-saveable 且非强制全驱逐时，跳过（等后台写 SSD）
    if (skipSsdSaveable && candidate->ssdSaveable() && !evictAllUnpinned) {
      continue;
    }
    acquireEvictedData(candidate, bytesToAcquire, acquired, ...);
    removeEntryLocked(candidate);
    ++numEvict_;
  }
}
```

**AccessStats 打分**（`AsyncDataCache.h` 约 `:72-102`）：

```cpp
struct AccessStats {
  tsan_atomic<AccessTime> lastUse{0};
  tsan_atomic<int32_t> numUses{0};
  int32_t score(AccessTime now, uint64_t size) const {
    if (!lastUse) return std::numeric_limits<int32_t>::max();
    return (now - lastUse) / (1 + numUses);  // 越久没用、用得越少 → 分越高越先驱逐
  }
};
```

### 1.2.4 内存容量与分配策略

**容量来源**：`AsyncDataCache` 本身没有独立的"缓存容量"字段——**总容量就是底层 `MemoryAllocator` 的 `capacity()`**。构造时传入的 allocator 决定了能吃多少内存：

```cpp
// AsyncDataCache.h:844
AsyncDataCache(
    const Options& options,
    memory::MemoryAllocator* allocator,
    std::unique_ptr<SsdCache> ssdCache = nullptr);
```

实际使用时，典型初始化路径（见 `velox/benchmarks/QueryBenchmarkBase.cpp:171-178`）：

```cpp
if (FLAGS_cache_gb) {
  memory::MemoryManager::Options options;
  int64_t memoryBytes = FLAGS_cache_gb * (1LL << 30);  // 单位 GB
  options.useMmapAllocator = true;
  options.allocatorCapacity = memoryBytes;             // ← 缓存大小来自这里
  options.useMmapArena = true;
  options.mmapArenaCapacityRatio = 1;
  memory::MemoryManager::testingSetInstance(options);
}
```

**容量判定**：`canTryAllocate`（`AsyncDataCache.cpp:1037-1048`）用 `allocator_->capacity() - allocator_->numAllocated()` 判断是否还有空间：

```cpp
bool AsyncDataCache::canTryAllocate(
    uint64_t requestBytes, const AcquiredMemory& acquired) const {
  const auto acquiredBytes = acquired.totalBytes();
  if (requestBytes <= acquiredBytes) return true;
  return requestBytes - acquiredBytes <=
      memory::AllocationTraits::pageBytes(
          memory::AllocationTraits::numPages(allocator_->capacity()) -
          allocator_->numAllocated());
}
```

**分配粒度**：allocator 按 4KB page 分配。

**Entry 三档存储形态**（由 size 自动选择，见 `AsyncDataCache.cpp:146-180` 的 `initialize`）：

```cpp
if (size_ < AsyncDataCacheEntry::kTinyDataSize) {  // < 2KB
  tinyData_.resize(size_);                         // 直接 std::string
  tinyData_.shrink_to_fit();
  return;
}
if (contiguous) {
  contiguousData_ = cache->allocator()->allocateBytes(size_);  // 连续
} else {
  cache->allocator()->allocateNonContiguous(sizePages, nonContiguousData_); // 页段
}
```

| Entry 大小 | 存储形态 | 分配方式 | 适用场景 |
|---|---|---|---|
| `< 2KB` | `std::string tinyData_` | 内联，不走 allocator | 元数据、小 column chunk |
| `≥ 2KB` 且要求连续 | `void* contiguousData_` | `allocateBytes()` | 需要 `memcpy`、解压目标 |
| `≥ 2KB` 允许非连续 | `memory::Allocation nonContiguousData_` | `allocateNonContiguous()` 返回多段 page runs | 大 chunk，避免碎片 |

**关键常量**：

| 常量 | 值 | 出处 |
|---|---|---|
| `kDefaultNumShards` | 4 | `AsyncDataCache.h:800` |
| `kExclusive` | -10000 | `AsyncDataCache.h:158` |
| `kTinyDataSize` | 2048 字节 | `AsyncDataCache.h:159` |
| Page size | 4KB | `MemoryAllocator` 约定 |

**Options 默认值**（`AsyncDataCache.h:802-813`）：

| 字段 | 默认 | 语义 |
|---|---|---|
| `maxWriteRatio` | **0.7** | 单批 SSD 写入最多占总 entry 数的 70%，防止 SSD 写卡住整 cache |
| `ssdSavableRatio` | **0.125** | 累计 ssdSaveable 页超过缓存页的 12.5% 才触发写 SSD |
| `minSsdSavableBytes` | **16MB** | 触发写 SSD 的最低绝对量（和 ratio 取 max） |
| `numShards` | **4** | 必须为 2 的幂 |
| `ssdFlushThresholdBytes` | **0（禁用）** | 可选的"硬阈值"，超过立即 flush |

### 1.2.5 驱逐策略：clock + 动态百分位阈值

**触发时机**：驱逐**不是后台定时任务**，而是**分配失败时同步触发**。当一次 `findOrCreate` 需要新内存、而 allocator 没空闲页时，走 `makeSpace` → 轮询各 shard 的 `evict`。

**clock 扫描主循环**（`AsyncDataCache.cpp:536-593`）：

```cpp
auto entryIndex = (clockHand_ % size);
auto iter = entries_.begin() + entryIndex;
while (++counter <= size) {
  if (++iter == entries_.end()) { iter = entries_.begin(); entryIndex = 0; }
  else { ++entryIndex; }
  ++clockHand_;                       // 永远推进

  int32_t score = candidate->score(now);
  if (candidate->numPins_ == 0 &&
      (evictAllUnpinned || score >= evictionThreshold_)) {
    if (skipSsdSaveable && candidate->ssdSaveable() && !evictAllUnpinned) {
      continue;                       // 等 SSD 写完再扔
    }
    acquireEvictedData(candidate, ...);
    removeEntryLocked(candidate);
    ++numEvict_;
  }
}
```

**动态阈值校准**（`AsyncDataCache.cpp:624-645` 的 `calibrateThresholdLocked`）：

```cpp
void CacheShard::calibrateThresholdLocked() {
  auto numSamples = std::min<int32_t>(kMaxEvictionSamples, entries_.size());
  evictionThreshold_ = percentile<int32_t>(
      [&]() -> int32_t { return iter->get() ? iter->get()->score(now) : 0; },
      numSamples,
      kEvictionPercentile);           // P80
}
```

- `kMaxEvictionSamples = 10`：随机抽 10 个 entry
- `kEvictionPercentile = 80`：取 80 分位数作为阈值
- 触发频率：每 `entries_.size() / 4` 次 evict 事件 或 `entries_.size() / 8` 次检查时重新校准

**这意味着**：阈值不是静态数字，而是**根据当前 workload 自适应**。如果 80% 的 entry 都很冷，阈值会自然升高，驱逐更激进；如果大部分 entry 都很热，阈值降低，驱逐保守。

**SSD-saveable 跳过逻辑**：当一个 entry 被标记 `ssdSaveable_` 但还没写入 SSD 时，普通驱逐会跳过它——等后台写完再扔，避免丢掉"刚要落到 SSD"的数据。只有 `evictAllUnpinned=true`（紧急模式）才会强制驱逐。

### 1.2.6 驱逐失败与内存压力应对

`makeSpace`（`AsyncDataCache.cpp:900-995`）是整个反压策略的总入口：

```cpp
bool AsyncDataCache::makeSpace(
    MachinePageCount numPages,
    std::function<bool(AcquiredMemory&)> allocate) {
  const int32_t kMaxAttempts = numShards_ * 4;    // 默认 4×4 = 16 次
  constexpr int32_t kMinEvictPages = 256;          // 最少驱逐 1MB
  constexpr int32_t kSmallSizePages = 2048;        // 8MB 以下算小请求
  float sizeMultiplier = 1.2;                      // 起步多驱逐 20%

  int32_t rank = 0;
  bool isCounted = false;
  for (auto nthAttempt = 0; nthAttempt < kMaxAttempts; ++nthAttempt) {
    if (canTryAllocate(requestBytes, acquired)) {
      if (allocate(acquired)) return true;
    }

    // 反压 1：SSD 正在写入 → 等 500ms
    if (nthAttempt > 2 && ssdCache_ && ssdCache_->writeInProgress()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    // 反压 2：尝试过半 → 开始计数竞争位次
    if (nthAttempt > kMaxAttempts / 2) {
      rank = ++numThreadsInAllocate_;
      isCounted = true;
    }
    if (rank) {
      acquired.free(allocator_);
      backoff(nthAttempt + rank);                  // 随机指数退避
    }

    // 轮询下一个 shard 做 evict
    shards_[shardCounter_++ & shardMask_]->evict(
        std::max<uint64_t>(kMinEvictPages, numPages) * sizeMultiplier,
        /*evictAllUnpinned=*/false,
        targetBytes - evictedBytes, acquired);

    // 小请求逐步加大 evict 量（1.2 → 2.4 → 4.8）
    if (numPages < kSmallSizePages && sizeMultiplier < 4) {
      sizeMultiplier *= 2;
    }
  }

  if (isCounted) { --numThreadsInAllocate_; }
  return false;                                    // 最终失败
}
```

**三档应对**：

| 阶段 | 行为 |
|---|---|
| 前 2 次 | 直接尝试 evict，小请求默认 1.2× |
| 第 3 ~ 8 次 | 如果 SSD 在写，等 500ms；evict 量加倍到 2.4× |
| 第 9 ~ 16 次 | 进入"竞争模式"：`++numThreadsInAllocate_` 给位次，按 `nthAttempt + rank` 做随机指数退避，避免多线程惊群 |

**最终失败**：如果 16 次都腾不出空间 → 返回 false，上层（`findOrCreate` 调用方）会走"无缓存"路径直接读 storage，**不让查询失败**——cache miss 但功能正常。

**强制驱逐（紧急模式）**：在 `CacheShard::evict` 时如果 `evictAllUnpinned=true`，会跳过所有阈值检查，强制扔掉未 pin 的 entry——这是内存极度紧张时的最后手段。

---

## 1.3 SSD 层：SsdCache / SsdFile

### 1.3.1 SsdCache 配置

定义在 `velox/common/caching/SsdCache.h:33`：

```cpp
class SsdCache {
 public:
  struct Config {
    std::string filePrefix;          // SSD 文件路径前缀
    uint64_t maxBytes;               // 总容量
    int32_t numShards;               // 分片数（同时是文件数）
    uint64_t checkpointIntervalBytes;// 每 N 字节做一次 checkpoint
    bool disableFileCow;             // 禁用 copy-on-write
    bool checksumEnabled;            // 写入校验和
    bool checksumReadVerificationEnabled;
    uint64_t maxEntries;             // 条目数上限，0=不限
    folly::Executor* executor;       // 异步写入用
  };
};
```

**文件布局**：
- 一共 `numShards` 个文件：`{filePrefix}0`, `{filePrefix}1`, ..., `{filePrefix}{numShards-1}`
- 每个文件内部按 **64MB region** 划分（`kRegionSize = 1 << 26`，见 `velox/common/caching/SsdFile.h`）
- `fileId % numShards` 决定一个 entry 落到哪个 SSD 文件

### 1.3.2 SsdRun：紧凑的 [offset|size] 编码

`velox/common/caching/SsdFile.h:39-92` 定义了 SSD 条目的打包表示：

```cpp
class SsdRun {
  static constexpr int32_t kSizeBits = 23;   // 最大 entry = 8MB
  uint64_t fileBits_;                        // [offset(41位) | size(23位)]
  uint32_t checksum_;

  uint64_t offset() const { return (fileBits_ >> kSizeBits); }
  uint32_t size() const { return (fileBits_ & ((1 << kSizeBits) - 1)) + 1; }
};
```

41 位 offset = 2TB 单文件寻址；23 位 size = 最大 8MB 单条目，这与内存层的 entry 大小保持一致。

### 1.3.3 异步写入流程

`velox/common/caching/SsdCache.cpp:97-149` 的 `SsdCache::write`：

```cpp
void SsdCache::write(std::vector<CachePin> pins) {
  std::vector<std::vector<CachePin>> shards(numShards_);
  // 1. 按 fileId % numShards 分发到各 SSD 文件
  for (auto& pin : pins) {
    shards[entry->key().fileId % numShards_].push_back(std::move(pin));
  }
  // 2. 每个分片独立异步提交
  for (auto i = 0; i < numShards_; ++i) {
    auto pinHolder = std::make_shared<PinHolder>(std::move(shards[i]));
    executor_->add([this, i, pinHolder, bytes, startTimeUs]() {
      try {
        files_[i]->write(pinHolder->pins);  // 实际写盘
      } catch (...) { /* 错误处理 */ }
      if (--writesInProgress_ == 0) { /* 日志 */ }
    });
  }
}
```

写入是**批量、异步、分片并行**的：调用方一次 submit 多个 pin，SsdCache 按 shard 分桶后投递到 folly executor，各 SSD 文件并发写入。

### 1.3.4 SSD 容量与文件增长

**容量来源**：`SsdCache::Config::maxBytes`，完全由配置决定（不同于内存层"继承" allocator 容量）。典型初始化（`velox/benchmarks/QueryBenchmarkBase.cpp:180-190`）：

```cpp
if (FLAGS_ssd_cache_gb) {
  constexpr int32_t kNumSsdShards = 16;                  // 16 个分片
  cacheExecutor_ = std::make_unique<folly::IOThreadPoolExecutor>(kNumSsdShards);
  const cache::SsdCache::Config config(
      FLAGS_ssd_path,
      static_cast<uint64_t>(FLAGS_ssd_cache_gb) << 30,   // GB → bytes
      kNumSsdShards,
      cacheExecutor_.get(),
      static_cast<uint64_t>(FLAGS_ssd_checkpoint_interval_gb) << 30);  // 默认 8GB
  ssdCache = std::make_unique<cache::SsdCache>(config);
}
```

**容量向上一级对齐**（`SsdCache.cpp:56-77`）：

```cpp
const uint64_t sizeQuantum = numShards_ * SsdFile::kRegionSize;   // 16 × 64MB = 1GB
const int32_t fileMaxRegions =
    bits::roundUp(config.maxBytes, sizeQuantum) / sizeQuantum;
```

即每个 SSD 文件的最大 region 数 = `maxBytes / (numShards × 64MB)`。例：`maxBytes=200GB`、`numShards=16` → 对齐到整 1GB 倍数。

**Lazy 增长**：SSD 文件不是启动时一次性预分配，而是**按需 grow**。`SsdFile::growOrEvictLocked`（`SsdFile.cpp:293-340`）先尝试扩文件，扩不了再淘汰：

```cpp
bool SsdFile::growOrEvictLocked() {
  if (numRegions_ < maxRegions_) {
    const auto newSize = (numRegions_ + 1) * kRegionSize;     // 64MB 对齐
    try {
      writeFile_->truncate(newSize);                          // ftruncate 扩容
      fileSize_ = newSize;
      writableRegions_.push_back(numRegions_);                // 新 region 可写
      ++numRegions_;
      return true;
    } catch (const std::exception& e) {
      ++stats_.growFileErrors;                                // 磁盘满，降级走淘汰
    }
  }

  if (state_.load() == State::kNoSpace) return false;         // 已经满状态

  // 走淘汰：挑 3 个 region 清空
  auto candidates = tracker_.findEvictionCandidates(
      kNumEvictionCandidates, numRegions_, regionPins_);      // kNumEvictionCandidates=3
  if (candidates.empty()) {
    suspended_ = true;                                        // 全被 pin → 挂起
    return false;
  }
  logEviction(candidates);                                    // 写 eviction log
  clearRegionEntriesLocked(candidates);                       // 清 in-memory 索引
  stats_.regionsEvicted += candidates.size();
  writableRegions_ = candidates;                              // 回收的 region 重新可写
  return true;
}
```

**关键常量**（`SsdFile.h`）：

| 常量 | 值 | 出处 | 语义 |
|---|---|---|---|
| `kRegionSize` | 64MB（`1 << 26`） | `SsdFile.h:320` | region 大小，淘汰/校验最小单位 |
| `kSizeBits` | 23 位 | `SsdFile.h:41` | size 字段位宽 → 单 entry 最大 8MB |
| `kNumEvictionCandidates` | 3 | `SsdFile.h:424` | 一次考虑几个 region 做淘汰 |

**默认配置汇总**：

| 参数 | 典型默认 | 说明 |
|---|---|---|
| `numShards` | 16 | benchmark 用 16，生产可调 |
| `checkpointIntervalBytes` | 8GB | 每 8GB 落一次 checkpoint |
| `maxEntries` | 0（不限） | 条目数软上限 |
| `disableFileCow` | false | 关 CoW（一些 FS 不支持可关掉） |
| `checksumEnabled` | false | 写时计算 checksum |
| `checksumReadVerificationEnabled` | false | 读时校验 |

### 1.3.5 SSD 写入触发链（从 entry 到 batch）

整个触发链是"**累计 + 阈值 + 去重**"的链式反应：

```
① entry 写完后 setExclusiveToShared()
        │ 标记 ssdSaveable_ = true（需 FileGroupStats 批准）
        ▼
② shard_->cache()->possibleSsdSave(size)   ← 每个标记都累计
        │
        ▼
③ AsyncDataCache::possibleSsdSave(bytes)   ← 全局累加 ssdSaveable_
        │
        │  满足任一阈值：
        │    a. ssdSaveable_ ≥ max(minSsdSavableBytes=16MB,
        │                          cachedPages × ssdSavableRatio=0.125)
        │    b. ssdSaveable_ ≥ ssdFlushThresholdBytes（默认禁用）
        ▼
④ ssdCache_->startWrite()   ← 抢"写锁"，保证只有一个写入批在跑
        │
        ▼
⑤ saveToSsd() → 各 shard 的 appendSsdSaveable()
        │  扫描所有 entry，收集 ssdSaveable_ && ssdFile_==nullptr 的
        │  单批最多 maxWriteRatio × shard.size() 个（默认 70%）
        ▼
⑥ ssdCache_->write(pins)
        │  按 fileId % numShards 分发到各 SsdFile
        ▼
⑦ executor_->add(...) 异步写每个 SsdFile
        │  写完后给 entry 挂上 ssdFile_/ssdOffset_
        ▼
⑧ writesInProgress_ 归零 → 日志 + 解锁
```

**核心阈值判定**（`AsyncDataCache.cpp:1073-1095`）：

```cpp
void AsyncDataCache::possibleSsdSave(uint64_t bytes) {
  if (ssdCache_ == nullptr) return;
  ssdSaveable_ += bytes;

  // 条件 A：动态阈值（ratio + 绝对值取 max）
  //   或条件 B：硬阈值 ssdFlushThresholdBytes
  if (memory::AllocationTraits::numPages(ssdSaveable_) >
          std::max<int32_t>(
              numPages(opts_.minSsdSavableBytes),           // 16MB
              static_cast<int32_t>(
                  static_cast<double>(cachedPages_) * opts_.ssdSavableRatio)) ||  // 12.5%
      (opts_.ssdFlushThresholdBytes > 0 &&
       numPages(ssdSaveable_) > numPages(opts_.ssdFlushThresholdBytes))) {
    if (!ssdCache_->startWrite()) return;                   // 已有写入在进行 → 跳过
    saveToSsd();
  }
}
```

**单批限量**（`AsyncDataCache.cpp:695-719` 的 `appendSsdSaveable`）：

```cpp
void CacheShard::appendSsdSaveable(bool saveAll, std::vector<CachePin>& pins) {
  std::lock_guard<std::mutex> l(mutex_);
  const int32_t limit = saveAll
      ? std::numeric_limits<int32_t>::max()
      : static_cast<int32_t>(
            static_cast<double>(entries_.size()) * maxWriteRatio_);  // 70%
  for (auto& entry : entries_) {
    if (entry && (entry->ssdFile_ == nullptr) && !entry->isExclusive() &&
        entry->ssdSaveable()) {
      ++entry->numPins_;
      pins.push_back(std::move(pin));
      if (pins.size() >= limit) break;                      // 防止 SSD 慢拖死 cache
    }
  }
}
```

**`maxWriteRatio=0.7` 的意义**：如果 SSD 慢于远端 storage，不能让 SSD 写卡住整 cache——最多 pin 70% entry 去写 SSD，剩下的留给 reader。

**FileGroupStats 决定单个 entry 能否写入 SSD**（`FileGroupStats.h:47-49`）：

```cpp
bool shouldSaveToSsd(uint64_t groupId, TrackingId trackingId) const {
  return true;   // 占位实现，真实策略可基于访问频率
}
```

`groupId` / `trackingId` 是 entry 的分组标签（文件级、scan 级等）。当前开源版本默认全放行，预留接口让生产环境按热度过滤。`AsyncDataCache::incrementNew` 会周期性调用 `updateSsdFilter(ssdMaxBytes × 0.9)`（`AsyncDataCache.cpp:1059-1071`）重新校准过滤策略。

### 1.3.6 SSD 侧淘汰：region-based + score-based

SSD 的淘汰单位不是单条 entry，而是**整个 64MB region**。这是 SSD 特性决定的——一次 `ftruncate` 的大段回收比逐条 overwrite 高效得多。

**SsdFileTracker 维护每个 region 的分数**（`SsdFileTracker.cpp:26-67`）：

```cpp
void SsdFileTracker::touch(int32_t region) {
  // 命中即拉高该 region 分数（最多到当前最高分的 1.1 倍）
  const auto best =
      *std::max_element(regionScores_.begin(), regionScores_.end());
  regionScores_[region] = std::max<double>(regionScores_[region], best * 1.1);
}

void SsdFileTracker::decay() {
  for (auto& score : regionScores_) {
    score = (score * 15) / 16;   // 周期性衰减，防止老 hot region 永远占着
  }
}

std::vector<int32_t> SsdFileTracker::findEvictionCandidates(
    int32_t numCandidates, int32_t numRegions,
    const std::vector<int32_t>& regionPins) {
  // 1. 算所有未 pin region 的平均分
  double scoreSum = 0; int32_t numUnpinned = 0;
  for (int i = 0; i < numRegions; ++i) {
    if (regionPins[i] > 0) continue;
    ++numUnpinned;
    scoreSum += regionScores_[i];
  }
  const auto avg = scoreSum / numUnpinned;

  // 2. 选分数 <= 平均的 region，按分数升序排，最多取 numCandidates 个
  std::vector<int32_t> candidates;
  for (auto i = 0; i < regionScores_.size(); ++i) {
    if ((regionPins[i] == 0) && (regionScores_[i] <= avg)) {
      candidates.push_back(i);
    }
  }
  std::sort(candidates.begin(), candidates.end(),
            [&](int32_t l, int32_t r) {
              return regionScores_[l] < regionScores_[r];
            });
  candidates.resize(std::min<int32_t>(candidates.size(), numCandidates));
  return candidates;
}
```

**关键设计**：
- **touch 时拉高到 max × 1.1**：保证刚命中的 region 不会立即被淘汰
- **decay 周期性衰减（× 15/16）**：长期不访问的 region 分数会逐步下降，最终被淘汰
- **平均分作阈值**：只淘汰"低于平均热度"的 region
- **pin 保护**：region 上有正在读的 entry 时，整 region 不可淘汰

**淘汰时的数据一致性**：
1. `logEviction(candidates)` 写 eviction log（持久化）——下次重启时读 checkpoint + 重放 eviction log 就能知道哪些 region 失效了
2. `clearRegionEntriesLocked` 清理 in-memory 的 entry map
3. `writableRegions_ = candidates` 回收的 region 加入可写队列
4. **被淘汰 region 里的 entry 不做"回写内存"**——内存里如果还有对应 CachePin，数据继续从内存读；内存里没有就等下次 cache miss 重读 storage

**SsdFile::write() 如何选位置**（`SsdFile.cpp:256-340` 的 `getSpace`）：

```cpp
std::optional<std::pair<uint64_t, int32_t>> SsdFile::getSpace(
    const std::vector<CachePin>& pins, int32_t begin) {
  for (;;) {
    if (writableRegions_.empty()) {
      if (!growOrEvictLocked()) return std::nullopt;        // 空间不够
    }
    const auto region = writableRegions_[0];
    const auto offset = regionSizes_[region];
    // 当前 region 装得下这批 pins 吗？
    //   装得下 → 分配 offset
    //   装不下 → 该 region 标记 filled，切下一个 region
    if (regionFilled) {
      tracker_.regionFilled(region);
      writableRegions_.erase(writableRegions_.begin());
    }
  }
}
```

**写入是顺序追加**：一个 region 装满后才切下一个，避免碎片。

### 1.3.7 Checkpoint 与重启恢复

**Checkpoint 机制**：每隔 `checkpointIntervalBytes` 字节做一次持久化（`SsdFile.h:509-519`）：

```cpp
bool SsdFile::needCheckpoint(bool force) const {
  if (!checkpointEnabled()) return false;
  if (state_.load() == State::kNoSpace) return false;   // SSD 满了不做 checkpoint
  return force || (bytesAfterCheckpoint_ >= checkpointIntervalBytes_);
}
```

Checkpoint 内容包括：
- 所有当前 entry 的 `{key → SsdRun}` 映射
- 文件元数据（region 数、大小）
- 校验和信息（如启用）

**Eviction log**：每次淘汰 region 时单独写一份 log，和 checkpoint 一起构成"checkpoint + 增量"的持久化模型。

**重启恢复**：

1. 打开 SSD 文件 → 读 checkpoint → 重建 `entries_` map
2. 重放 eviction log → 剔除已淘汰 region 的 entry
3. 校验 checksum（如启用）
4. 从上次 checkpoint 点继续写

**Shutdown 流程**（`SsdCache.cpp:210-227`）：

```cpp
void SsdCache::shutdown() {
  {
    std::lock_guard<std::mutex> l(mutex_);
    shutdown_ = true;
  }
  while (writesInProgress_) {
    std::this_thread::sleep_for(kWriteWaitMs);   // 每 100ms 轮询，等写完
  }
  for (auto& file : files_) {
    file->checkpoint(true);                      // 强制最后一次 checkpoint
  }
}
```

**Suspended / NoSpace 状态**：当 `growOrEvictLocked` 失败（所有 region 都被 pin，或磁盘满），`SsdFile` 进入 `suspended_` 或 `kNoSpace` 状态，此时读 SSD 直接返回 miss，不参与写入。等下次有 region 释放时会自动恢复。

---

## 1.4 级联查找：Memory → SSD → Storage

真正体现"级联"语义的是 `CachedBufferedInput::load`，位于 `velox/dwio/common/CachedBufferedInput.cpp:221-273`：

```cpp
void CachedBufferedInput::load(const LogType) {
  for (auto& request : requests) {
    // ① 先看内存
    if (cache_->exists(part->key)) continue;

    // ② 再看 SSD
    if (ssdFile != nullptr) {
      part->ssdPin = ssdFile->find(part->key);
      if (!part->ssdPin.empty()) {
        ssdLoad[loadIndex].push_back(part);   // SSD 命中
        continue;
      }
    }

    // ③ 最后才下发到底层存储
    storageLoad[loadIndex].push_back(part);
  }
}
```

**三层去重新编码的视角**：

```
┌──────────────────────────────────────────────────────────┐
│  ① 内存命中  (CachePin 持有 AsyncDataCacheEntry)          │
│     → 直接走 0 字节 IO                                    │
├──────────────────────────────────────────────────────────┤
│  ② SSD 命中  (SsdPin 持有 SsdRun)                         │
│     → 从 SSD 文件读回内存, 生成新的 CacheEntry           │
│     → CoalescedLoad::loadOrFuture() 触发                  │
├──────────────────────────────────────────────────────────┤
│  ③ Storage miss  → ReadFileInputStream::read / readAsync  │
│     → 读完后写入内存, 顺带标记 ssdSaveable                 │
└──────────────────────────────────────────────────────────┘
```

读 SSD 的路径由 `CoalescedLoad` 统一调度（见下一节），合并邻近 key 的 SSD 读请求。

---

## 1.5 IO 合并：CoalesceIo

### 1.5.1 通用合并模板

`velox/common/base/CoalesceIo.h:26-133` 定义了一个**完全泛型**的合并算法，核心思路是"邻近的小 IO 合并成大 IO，中间的 gap 用 skip 标记带过"：

```cpp
template <typename Item, typename Range, typename...>
CoalesceIoStats coalesceIo(
    const std::vector<Item>& items,
    int32_t maxGap,          // 合并的最大间隔（字节）
    int32_t rangesPerIo,     // 单次 IO 最大 range 数
    ItemOffset offsetFunc,
    ItemSize sizeFunc,
    ItemNumRanges numRanges,
    AddRanges addRanges,
    SkipRange skipRange,     // gap 用 nullptr 标记
    IoFunc ioFunc);
```

算法主循环：

```cpp
for (int32_t i = 0; i < items.size(); ++i) {
  const auto itemOffset = offsetFunc(i);
  const auto itemSize = sizeFunc(i);
  const int64_t gap = itemOffset - lastEndOffset;

  if (gap > 0 && gap < maxGap && !enoughRanges) {
    result.extraBytes += gap;        // 多读了 gap 字节
    skipRange(gap, ranges);          // 但会用 nullptr 标记丢弃
  } else {
    ioFunc(items, startItem, i, startOffset, ranges);  // 提交一组
    ranges.clear();
    ++result.numIos;
  }
  addRanges(item, ranges);
  lastEndOffset = itemOffset + itemSize;
}
```

### 1.5.2 readPins：把模板用到 cache pin 上

`AsyncDataCache.cpp:1195-1244` 的 `readPins` 把上述模板具体化到 `CachePin` 序列：

```cpp
CoalesceIoStats readPins(
    const std::vector<CachePin>& pins,
    int32_t maxGap, int32_t rangesPerIo,
    std::function<uint64_t(int32_t)> offsetFunc,
    std::function<void(...)> readFunc) {
  return coalesceIo<CachePin, folly::Range<char*>>(
      pins, maxGap, rangesPerIo,
      std::move(offsetFunc),
      [&](int32_t i) { return pins[i].checkedEntry()->size(); },
      [&](int32_t i) {
        return std::max<int32_t>(
            1, pins[i].checkedEntry()->nonContiguousData().numRuns());
      },
      // addRanges：连续 vs 非连续不同处理
      [&](const CachePin& pin, std::vector<folly::Range<char*>>& ranges) {
        if (entry->hasContiguousData()) {
          ranges.push_back(folly::Range<char*>(entry->contiguousData(), size));
        } else {
          for (auto i = 0; i < data.numRuns(); ++i) {
            ranges.push_back(folly::Range<char*>(run.data<char>(), readSize));
          }
        }
      },
      // skipRange：用 nullptr+size 标记
      [&](int32_t size, std::vector<folly::Range<char*>>& ranges) {
        ranges.push_back(folly::Range<char*>(
            nullptr, reinterpret_cast<char*>(static_cast<uint64_t>(size))));
      },
      std::move(readFunc));
}
```

**合并效果示意**：

```
请求序列（offset 连续，gap=8KB）:
  Req0: [1000, 1100)
  Req1: [1108, 1200)   ← gap=8
  Req2: [1300, 1400)   ← gap=100
  Req3: [1500, 1600)   ← gap=100
  ──────────────────────────────────
  maxGap = 1MB → 全部合并为 1 个 IO
  Result: read [1000, 1600) 一次性读 600B，
          其中 Req0/1/2/3 的边界通过 range 数组精准对齐
```

### 1.5.3 统计量

`CoalesceIoStats`（`CoalesceIo.h:31-48`）记录：

- `numIos`：实际发出的 IO 数
- `payloadBytes`：有用的字节数
- `extraBytes`：因合并多读的 gap 字节
- `duplicateRegions`/`duplicateBytes`：重复请求（同 offset+size）

---

## 1.6 预读 / Readahead / Preload

### 1.6.1 shouldPreload：是否进入预读

`velox/dwio/common/CachedBufferedInput.cpp:86-111`：

```cpp
bool CachedBufferedInput::shouldPreload(int32_t numPages) {
  if (requests_.empty() && (numPages == 0)) return false;

  for (const auto& request : requests_) {
    numPages += memory::AllocationTraits::numPages(
        std::min<int32_t>(request.size, options_.loadQuantum()));
  }

  const auto cachePages = cache_->cachedPages();
  auto* allocator = cache_->allocator();
  const auto maxPages = memory::AllocationTraits::numPages(allocator->capacity());
  const auto allocatedPages = allocator->numAllocated();

  // 条件 A：分配后还有空闲页 → 预读
  if (numPages < maxPages - allocatedPages) return true;

  // 条件 B：预读后还没占满缓存一半 → 预读
  const auto prefetchPages = cache_->incrementPrefetchPages(0);
  if (numPages + prefetchPages < cachePages / 2) return true;

  return false;
}
```

这是**自适应预读**的核心门槛——不是无条件预读，而是综合考量当前内存压力、已预读但未消费的页数，避免预读反噬命中区。

### 1.6.2 基于 TrackingData 的智能分片

`CachedBufferedInput.cpp:119-153` 的 `makeRequestParts` 会根据一个文件的**历史访问密度**决定是否切分大请求为更小的预读块：

```cpp
std::vector<CacheRequest*> makeRequestParts(
    CacheRequest& request,
    const cache::TrackingData& trackingData,
    int32_t loadQuantum,
    std::vector<std::unique_ptr<CacheRequest>>& extraRequests) {
  // readDensity = 实际读 / 引用读；>0.8 认为是稠密访问
  const auto readDensity =
      trackingData.readBytes / (1 + trackingData.referencedBytes);
  const auto readPct = 100 * readDensity;
  const bool prefetch = trackingData.referencedBytes > 0 &&
      isPrefetchPct(readPct) && readDensity >= 0.8;

  for (uint64_t offset = 0; offset < request.size; offset += loadQuantum) {
    parts.push_back(std::make_unique<CacheRequest>(...));
    parts.back()->coalesces = prefetch;   // 稠密才允许后续合并
  }
  return parts;
}
```

**含义**：
- 稀疏访问（selective column）→ 切成 `loadQuantum` 块，各自独立 IO，不浪费带宽
- 稠密访问（大范围扫描）→ 切成块但允许合并，相当于一次大 IO

`TrackingData` 由 `ScanTracker`（`velox/common/caching/ScanTracker.h`）持续累积，跨 query 共享。

### 1.6.3 异步预读执行

`CachedBufferedInput.cpp:579-589`：

```cpp
if (prefetch && executor_) {
  for (auto i = startIndex; i < coalescedLoads_.size(); ++i) {
    auto& load = coalescedLoads_[i];
    if (load->state() == CoalescedLoad::State::kPlanned) {
      executor_->add([pendingLoad = load, ssdSavable = options_.cacheable()]() {
        pendingLoad->loadOrFuture(nullptr, ssdSavable);
      });
    }
  }
}
```

State 为 `kPlanned` 的 `CoalescedLoad` 被丢到 folly executor 异步执行；当下次 reader 真正访问时，数据已经在内存里了。

---

## 1.7 并发、Sharding 与反压

### 1.7.1 Sharding：减少锁争用

`AsyncDataCache` 默认 4 shards（`kDefaultNumShards = 4`，`AsyncDataCache.h:800`）。每个 shard 有独立的：

- `mutex` 保护 `entries_` map
- `clockHand_` 和 `evictionThreshold_` 独立推进
- 独立的 `newEntries_`、`numEvict_` 等计数器

**分片路由**：`shardIdx = hash(fileId, offset) & (numShards - 1)`，保证同一 IO 的多个 entry 落在相同 shard，方便合并。

### 1.7.2 CoalescedLoad 状态机：避免重复 IO

`velox/common/caching/AsyncDataCache.h:479-539` 的 `CoalescedLoad` 是去重器：

```cpp
class CoalescedLoad {
 public:
  enum class State { kPlanned, kLoading, kCancelled, kLoaded };

  bool loadOrFuture(folly::SemiFuture<bool>* wait, bool ssdSavable) {
    std::lock_guard<std::mutex> l(mutex_);
    if (state_ == State::kLoading) {
      // 已有线程在 load，挂个 future 等
      if (wait != nullptr) {
        *wait = promise_->getSemiFuture();
      }
      return false;
    }
    state_ = State::kLoading;
  }
  // mutex 外执行实际 load
  const auto pins = loadData(/*prefetch=*/wait == nullptr);
  for (const auto& pin : pins) {
    pin.checkedEntry()->setExclusiveToShared(ssdSavable);
  }
  setEndState(State::kLoaded);
}
```

**并发场景**：两个 reader 同时请求同一批 region → 第一个进入 `kLoading`，第二个拿到 future 等待；完成后所有 entry 转 shared，双方都能读到。**等效于 IO 级别的 dedup**。

### 1.7.3 makeSpace：反压与回退

当内存吃紧时，`AsyncDataCache::makeSpace`（`AsyncDataCache.cpp:900-995`）展现了完整的反压策略：

```cpp
bool AsyncDataCache::makeSpace(
    MachinePageCount numPages,
    std::function<bool(AcquiredMemory&)> allocate) {
  const int32_t kMaxAttempts = numShards_ * 4;
  int32_t rank = 0;   // 排队竞争位次

  for (auto nthAttempt = 0; nthAttempt < kMaxAttempts; ++nthAttempt) {
    if (canTryAllocate(requestBytes, acquired)) {
      if (allocate(acquired)) return true;
    }

    // 反压 1：SSD 写入未完成时，等它一会
    if (nthAttempt > 2 && ssdCache_ && ssdCache_->writeInProgress()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    // 反压 2：有竞争 → 指数退避
    if (rank) {
      acquired.free(allocator_);
      backoff(nthAttempt + rank);
    }

    // 轮询驱逐下一个 shard
    shards_[shardCounter_++ & shardMask_]->evict(...);
  }
  return false;
}
```

`backoff` 实现（`AsyncDataCache.cpp:1050-1057`）：

```cpp
void AsyncDataCache::backoff(int32_t counter) {
  size_t seed = folly::hasher<uint16_t>()(++backoffCounter_);
  const auto usecs = (seed & 0xfff) * (counter & 0x1f);
  std::this_thread::sleep_for(std::chrono::microseconds(usecs));
}
```

**两层反压**：
1. **SSD 写入反压**：等 evict-to-SSD 完成（500ms 粒度）
2. **竞争反压**：多线程同时 makeSpace 时，通过 `rank` 错开重试时机

### 1.7.4 SSD 写并发控制

`AsyncDataCache.h:815-831` 的 `maxWriteRatio`：当正在写 SSD 的 entry 数 / 总 entry 数超过该比例（默认 0.7）时，**停止写入新 entry 到 SSD**。防止 SSD 被写得过快而内存侧驱逐不掉。

---

## 1.8 其他缓存机制：TTL / FileGroupStats / 统计 / 文件注册

### 1.8.1 CacheTTLController：按文件老化清理

`velox/common/caching/CacheTTLController.h:44-103` 提供一个进程级单例，用于按文件生命周期清理缓存：

```cpp
class CacheTTLController {
 public:
  static CacheTTLController* create(AsyncDataCache& cache);

  // 记录文件被打开的时间
  bool addOpenFileInfo(uint64_t fileNum, int64_t openTimeSec = getCurrentTimeSec());

  // 查询当前 cache 的年龄分布
  CacheAgeStats getCacheAgeStats() const;

  // 应用 TTL：把所有 open 时间早于 ttlSecs 的文件的 entry 清掉
  void applyTTL(int64_t ttlSecs);

 private:
  folly::F14FastSet<uint64_t> getAndMarkAgedOutFiles(int64_t maxOpenTimeSecs);
  void cleanUp(const folly::F14FastSet<uint64_t>& filesToRetain);
};
```

**使用场景**：当文件被覆盖（例如 ETL 重写分区）、或远端文件被删除时，cache 里对应的 entry 就成了"幽灵"。TTLController 根据 open 时间把老 entry 清理掉，**同时清内存 cache 和 SSD cache**。

### 1.8.2 FileGroupStats：分组决定 SSD 准入

`velox/common/caching/FileGroupStats.h:24-61`：

```cpp
class FileGroupStats {
 public:
  void recordReference(uint64_t fileId, uint64_t groupId,
                       TrackingId trackingId, int32_t bytes);
  void recordRead(uint64_t fileId, uint64_t groupId,
                  TrackingId trackingId, int32_t bytes);
  void recordFile(uint64_t fileId, uint64_t groupId, int32_t numStripes);

  bool shouldSaveToSsd(uint64_t groupId, TrackingId trackingId) const;

  // 当 cache 换过一定量，重新校准 SSD 准入过滤
  void updateSsdFilter(uint64_t ssdSize, int32_t decayPct = 0);
};
```

**工作模型**：
- `groupId`：逻辑分组（表 / 分区 / 文件组），由调用方指定
- `trackingId`：细粒度追踪（某次 scan、某条 stream）
- 每次 reference（引用）和 read（实际读）都记录，算出"读密度"
- `shouldSaveToSsd()` 是真实 SSD 准入判定的入口；开源版本默认 true，生产侧可插入策略

**SSD 准入过滤器周期更新**（`AsyncDataCache.cpp:1059-1071` 的 `incrementNew`）：

```cpp
void AsyncDataCache::incrementNew(uint64_t size) {
  newBytes_ += size;
  if (ssdCache_ == nullptr) return;
  if (newBytes_ > nextSsdScoreSize_) {
    // 每换掉 max(1 cache size, 256MB) 数据就重新校准一次
    nextSsdScoreSize_ = newBytes_ +
        std::max<int64_t>(
            memory::AllocationTraits::pageBytes(cachedPages_),
            1UL << 28);
    ssdCache_->groupStats().updateSsdFilter(ssdCache_->maxBytes() * 0.9);
  }
}
```

这保证随着 workload 变化，"哪些 group 值得写到 SSD" 的判定会动态调整。

### 1.8.3 ScanTracker：跨 scan 共享访问模式

`velox/common/caching/ScanTracker.h` 把**每个 scan** 的数据访问模式聚合起来：

- `TrackingData`（referencedBytes、readBytes）由 reader 每次 enqueue / load 时记录
- `CachedBufferedInput::makeRequestParts`（见 §1.6.2）用 `readDensity` 判定是否合并预读
- 同一个 scan 的多次访问累加；跨 query 的同表访问由 `FileGroupStats` 聚合

### 1.8.4 FileIds 与 StringIdLease：文件注册表

`velox/common/caching/FileIds.h` 和 `StringIdMap.{h,cpp}` 提供：

- `fileNum` 是 cache key 的核心字段（和 offset 一起唯一确定一个 entry）
- `StringIdLease`：引用计数的字符串 ↔ uint64 映射，避免在 key 里存长字符串
- 文件第一次被 cache 引用时注册，引用归零后回收

`FileHandle`（`velox/common/caching/FileHandle.h`）封装文件路径、ID、长度、last modified time 等信息，保证 cache key 能区分同一 path 的不同版本。

### 1.8.5 命中率与统计

`AsyncDataCache.cpp:279-286` 展示 hit/miss 计数细节：

```cpp
foundEntry->touch();                          // 更新 lastUse/numUses
if (foundEntry->isPrefetch()) {
  foundEntry->isFirstUse_ = true;
  foundEntry->setPrefetch(false);
} else {
  ++numHit_;                                  // 真正的命中
  hitBytes_ += foundEntry->size();
}
```

**Prefetch 首次命中不计入 hit**：因为 entry 是预读放进来的，第一次访问只是"消费预读"，不能算真正的命中——这样统计出来的 hit rate 更能反映 reader 的实际 cache 利用情况。

`CacheStats` 字段（在 `CacheShard` 和 `AsyncDataCache` 上各一份）：

| 计数器 | 含义 |
|---|---|
| `numHit_` / `hitBytes_` | 命中次数 / 字节 |
| `numNew_` / `newBytes_` | 新建 entry 次数 / 字节 |
| `numEvict_` | 驱逐次数 |
| `numPrefetchPages_` | 当前预读未消费的页数 |
| `allocClocks_` | allocator 分配/释放累计时间 |

### 1.8.6 缓存生命周期

```
   进程启动
       │
       ▼
   MemoryManager::testingSetInstance(allocatorCapacity=FLAGS_cache_gb << 30)
       │
       ▼
   AsyncDataCache::create(allocator, ssdCache, Options{})
       │                                       │
       │                                       └─ ssdCache 从 path 加载 checkpoint
       │                                           重建 entries_ map
       ▼
   查询运行：findOrCreate / read / evict / saveToSsd 周而复始
       │
       ▼
   AsyncDataCache::shutdown()
       │
       ├─ ssdCache_->shutdown()  → 等 writesInProgress 归零
       │                          → 各 SsdFile checkpoint(force=true)
       ├─ shard->shutdown()      → 清 entries_
       └─ allocator 仍存活（归 MemoryManager 管）
```

---

# 第二部分：上层格式读取的 IO 并发

## 2.1 抽象层次总览

从 SQL 查询到磁盘 IO，Velox 的层次非常清晰：

```
┌─────────────────────────────────────────────────────────┐
│  exec::TableScan  (SourceOperator)                      │
│    - 产出 RowVector                                     │
│    - 通过 DataSource 拉数据                             │
└──────────────────┬──────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────┐
│  connector::hive::HiveDataSource / FileDataSource       │
│    - 封装 split、剩余 filter、输出类型                   │
│    - 内含 FileSplitReader                               │
└──────────────────┬──────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────┐
│  dwio::common::Reader  (格式无关接口)                    │
│     └─ ParquetReader / DwrfReader / NimbleReader        │
│        └─ RowReader                                     │
│           └─ StructColumnReader / SelectiveColumnReader │
└──────────────────┬──────────────────────────────────────┘
                   │
                   │  发起 Region 级读请求
                   ▼
┌─────────────────────────────────────────────────────────┐
│  dwio::common::BufferedInput / CachedBufferedInput      │
│    - enqueue(region)  收集请求                           │
│    - load()           统一调度（合并 + 缓存 + 预读）      │
└──────────────────┬──────────────────────────────────────┘
                   │
                   ▼
            ┌──────────────────┐
            │  AsyncDataCache  │  (第一部分已详述)
            │  + SsdCache      │
            └──────────────────┘
```

本部分关注从 **Reader 到 BufferedInput** 这段——即格式层如何组织请求、如何并发、如何预读。

---

## 2.2 Reader / RowReader / ColumnReader 三层体系

### 2.2.1 抽象接口

`velox/dwio/common/Reader.h` 定义了三层接口：

```cpp
class Reader {
  virtual std::unique_ptr<RowReader> createRowReader(
      const RowReaderOptions& options = {}) const = 0;
  virtual std::optional<uint64_t> numberOfRows() const = 0;
  virtual const velox::RowTypePtr& rowType() const = 0;
};

class RowReader {
  virtual uint64_t next(uint64_t size, VectorPtr& result,
                        const Mutation* mutation = nullptr) = 0;
  virtual int64_t nextRowNumber() = 0;
  virtual void updateRuntimeStats(RuntimeStatistics& stats) const = 0;
};
```

### 2.2.2 具体格式实现

| 格式 | Reader | RowReader | 文件位置 |
|---|---|---|---|
| Parquet | `ParquetReader` | `ParquetRowReader` | `velox/dwio/parquet/reader/ParquetReader.{h,cpp}` |
| DWRF | `DwrfReader` | `DwrfRowReader` | `velox/dwio/dwrf/reader/DwrfReader.{h,cpp}` |
| Nimble | `NimbleReader` | `NimbleRowReader` | `velox/dwio/nimble/reader/` |

每个格式都共享同样的 `BufferedInput` 抽象，但各自的元数据加载、split 策略、并发粒度不同。

---

## 2.3 BufferedInput 与 CachedBufferedInput

### 2.3.1 BufferedInput：两阶段加载

`velox/dwio/common/BufferedInput.h` 定义了核心抽象。它是**两阶段 API**：

```cpp
class BufferedInput {
  // Phase 1：声明请求（不立即 IO）
  virtual std::unique_ptr<SeekableInputStream> enqueue(
      velox::common::Region region, const StreamIdentifier* sid = nullptr);

  // Phase 2：统一加载（触发真正的 IO，会做合并）
  virtual void load(const LogType);

  // 立即读（非计划路径，用于小元数据等）
  virtual std::unique_ptr<SeekableInputStream> read(
      uint64_t offset, uint64_t length, LogType logType) const;

  // 预读相关
  virtual bool shouldPreload(int32_t numPages = 0);
  virtual bool shouldPrefetchStripes() const;
};
```

**两阶段的价值**：reader 先 `enqueue` 所有要读的 column region，再 `load` 一次——`load` 内部有机会合并、去重、预读。如果每次 read 都立即发 IO，就无法跨 column 合并了。

### 2.3.2 CachedBufferedInput：带缓存的实现

`velox/dwio/common/CachedBufferedInput.h` 是生产环境使用的实现。它扩展了 `BufferedInput`，把所有请求转化为 cache key（`{fileId, offset}`）并委托给 `AsyncDataCache`：

```cpp
class CachedBufferedInput : public BufferedInput {
  std::unique_ptr<SeekableInputStream> enqueue(...) override;
  void load(const LogType) override;

  bool prefetch(velox::common::Region region);
  bool shouldPreload(int32_t numPages = 0) override;

  cache::AsyncDataCache* cache() const;
  std::optional<CachedRegion> findCachedRegion(uint64_t offset) const;

 private:
  // 合并邻近请求分组
  template <bool kSsd>
  std::vector<int32_t> groupRequests(
      const std::vector<CacheRequest*>& requests, bool prefetch) const;

  void readRegions(const std::vector<CacheRequest*>& requests,
                   bool prefetch,
                   const std::vector<int32_t>& groupEnds);
};
```

### 2.3.3 CacheRequest 数据结构

`velox/dwio/common/CachedBufferedInput.h:32-51`：

```cpp
struct CacheRequest {
  cache::RawFileCacheKey key;   // {fileId, offset}
  uint64_t size;                // 读取字节数
  cache::TrackingId trackingId; // 流标识（用于统计访问模式）
  cache::CachePin pin;          // 内存 cache pin
  cache::SsdPin ssdPin;         // SSD cache pin

  bool coalesces{true};         // 是否参与合并
  const SeekableInputStream* stream;
};
```

`coalesces` 字段控制合并行为——稀疏列读到 loadQuantum 边界时会置为 false（见 §1.6.2）。

---

## 2.4 Parquet：Row Group 调度与列并发

### 2.4.1 类层次

```
ParquetReader
   └─ ReaderBase  (共享元数据、BufferedInput map)
        └─ ParquetRowReader::Impl
              └─ StructColumnReader  (顶层 struct，包含所有列)
                    ├─ ColumnReader  (叶子列：Int64/String/...)
                    └─ StructColumnReader / ListColumnReader (嵌套)
```

`ReaderBase` 持有一个 `std::unordered_map<uint32_t, std::shared_ptr<BufferedInput>> inputs_`，key 是 row group 序号——**每个 row group 有独立的 BufferedInput**，这是并发的前提。

### 2.4.2 scheduleRowGroups：预读多个 row group

`velox/dwio/parquet/reader/ParquetReader.cpp:1344-1361`：

```cpp
void ReaderBase::scheduleRowGroups(
    const std::vector<uint32_t>& rowGroupIds,
    int32_t currentGroup,
    StructColumnReader& reader) {
  // 一次预读 prefetchRowGroups + 1 个 row group
  auto numRowGroupsToLoad = std::min(
      options_.prefetchRowGroups() + 1,
      static_cast<int64_t>(rowGroupIds.size() - currentGroup));

  for (auto i = 0; i < numRowGroupsToLoad; i++) {
    auto thisGroup = rowGroupIds[currentGroup + i];
    if (!inputs_[thisGroup]) {
      inputs_[thisGroup] = reader.loadRowGroup(thisGroup, input_);
    }
  }

  // 已经走过的上一个 row group，释放掉其 BufferedInput
  if (currentGroup >= 1) {
    inputs_.erase(rowGroupIds[currentGroup - 1]);
  }
}
```

**并发机制**：
- `prefetchRowGroups` 控制预读窗口大小（默认值在 `ReaderOptions`）
- 当前 row group 正在被解码时，下一批 row group 的 IO 已经在 executor 上飞
- 上一个 row group 处理完立即 `erase`，释放内存

### 2.4.3 loadRowGroup：列递归 enqueue

`StructColumnReader::loadRowGroup`（在 `velox/dwio/parquet/reader/StructColumnReader.cpp`）：

```cpp
std::shared_ptr<BufferedInput> StructColumnReader::loadRowGroup(
    uint32_t index, const std::shared_ptr<BufferedInput>& input) {
  if (isRowGroupBuffered(index, *input)) {
    enqueueRowGroup(index, *input);    // 已在缓存 → 仅 enqueue
    return input;
  }

  auto newInput = input->clone();      // 独立的 BufferedInput
  enqueueRowGroup(index, *newInput);
  newInput->load(LogType::STRIPE);     // 触发该 row group 的所有列 IO
  return newInput;
}

void StructColumnReader::enqueueRowGroup(uint32_t index, BufferedInput& input) {
  // 递归：让所有 child column 把自己的 column chunk region 加进来
  for (auto& child : children_) {
    child->enqueueRowGroup(index, input);
  }
}
```

**关键点**：**同一个 row group 内所有列的 region 共享一个 BufferedInput 实例**，所以 `load()` 调用一次就把这些列的 IO 合并下发。这就是 Parquet 列存"一次合并多个列"的并发模型。

### 2.4.4 PageReader：页级读取

数据真正解码时进入 `PageReader`（`velox/dwio/parquet/reader/PageReader.{h,cpp}`）：

```cpp
class PageReader {
  template <typename Visitor>
  void readWithVisitor(Visitor& visitor);   // 按页解码 + 过滤
  void skip(int64_t numRows);                // 跳过行

  const VectorPtr& dictionaryValues(const TypePtr& type);

 private:
  void seekToPage(int64_t row);
  void prepareDataPageV1(const thrift::PageHeader& pageHeader, int64_t row);
  void prepareDataPageV2(const thrift::PageHeader& pageHeader, int64_t row);
  const char* decompressData(const char* pageData,
                             uint32_t compressedSize,
                             uint32_t uncompressedSize);
};
```

页级别已经不直接产生 IO——PageReader 从 column chunk 的 `SeekableInputStream`（即 `CacheInputStream`）顺序读取。**IO 全部发生在 `load()` 阶段**，PageReader 只负责解码。

### 2.4.5 Parquet 整体流程

```
1. 打开文件：读 footer (last N bytes)
        │
        ▼
2. 解析 FileMetaData：拿到所有 row group 的元数据
        │
        ▼
3. 按 split 范围、列裁剪、统计信息过滤 row groups
        │
        ▼
4. 进入 next() → scheduleRowGroups(currentGroup)
        │  → 对 [current, current+prefetchRowGroups] 每个 row group:
        │      StructColumnReader::loadRowGroup
        │        → 各 child column 递归 enqueue column chunk region
        │        → newInput->load(STRIPE)
        │           → CachedBufferedInput::load
        │              → 内存/SSD/storage 三级查找
        │              → CoalescedLoad::loadOrFuture 异步
        ▼
5. PageReader 从 cache stream 读页
        │
        ▼
6. SelectiveColumnReader::readWithVisitor 解码 + 过滤
        │
        ▼
7. 产出到 RowVector
```

---

## 2.5 DWRF/ORC：Stripe 与 UnitLoader

### 2.5.1 类层次

```
DwrfReader
   └─ ReaderBase  (PostScript, Footer, StripeMetadataCache, 解密 handler)
        └─ DwrfRowReader : StripeReaderBase
              ├─ UnitLoader       (调度 stripe 加载/卸载)
              │    └─ DwrfUnit[]  (每个 stripe 一个 unit)
              └─ 当前 stripe 的 ColumnReader tree
```

### 2.5.2 DwrfUnit：单 stripe 的加载单元

`velox/dwio/dwrf/reader/DwrfReader.cpp:39-147`：

```cpp
class DwrfUnit : public LoadUnit {
  void load() override {
    ensureDecoders();   // 建 column reader tree
    loadDecoders();     // 实际触发 IO 并解码
  }

  void unload() override {
    cachedIoSize_.reset();
    stripeStreams_.reset();
    columnReader_.reset();
    selectiveColumnReader_.reset();
    stripeDictionaryCache_.reset();
    stripeReadState_.reset();
  }

  uint64_t getNumRows() override { return stripeInfo_.numberOfRows(); }

  uint64_t getIoSize() override {
    if (cachedIoSize_) return *cachedIoSize_;
    ensureDecoders();
    cachedIoSize_ =
        stripeReadState_->stripeMetadata->stripeInput->nextFetchSize();
    return *cachedIoSize_;
  }

 private:
  void ensureDecoders() {
    if (columnReader_ || selectiveColumnReader_) return;
    // 取 stripe 元数据 → 创建 StripeReadState → 构建 reader tree
    stripeReadState_ = std::make_shared<StripeReadState>(
        stripeReaderBase_.readerBaseShared(),
        stripeReaderBase_.fetchStripe(stripeIndex_, preloaded_));
    stripeStreams_ = std::make_unique<StripeStreamsImpl>(...);
    if (scanSpec) {
      selectiveColumnReader_ = SelectiveDwrfReader::build(...);
    } else {
      columnReader_ = factory->build(...);
    }
  }
};
```

`load()` / `unload()` 的对称设计让 UnitLoader 可以**像滑动窗口一样**管理 stripe：进的来 decode，出去的立即释放内存。

### 2.5.3 UnitLoader：滑动窗口策略

`velox/dwio/common/UnitLoader.h` 定义抽象接口：

```cpp
class LoadUnit {
  virtual void load() = 0;
  virtual void unload() = 0;
  virtual uint64_t getNumRows() = 0;
  virtual uint64_t getIoSize() = 0;
};

class UnitLoader {
  virtual LoadUnit& getLoadedUnit(uint32_t unit) = 0;
  virtual void onRead(uint32_t unit, uint64_t rowOffsetInUnit, uint64_t rowCount) = 0;
  virtual void onSeek(uint32_t unit, uint64_t rowOffsetInUnit) = 0;
};
```

典型实现 `OnDemandUnitLoader`（`velox/dwio/common/OnDemandUnitLoader.cpp`）只保持一个 unit 在内存：

```cpp
LoadUnit& OnDemandUnitLoader::getLoadedUnit(uint32_t unit) override {
  if (loadedUnit_ && *loadedUnit_ == unit) {
    return *loadUnits_[unit];
  }
  if (loadedUnit_) {
    loadUnits_[*loadedUnit_]->unload();   // 卸载旧 unit
  }
  loadUnits_[unit]->load();
  loadedUnit_ = unit;
  return *loadUnits_[unit];
}
```

更复杂的实现（`QuantumBlockingUnitLoader` 等）会**预读多个 stripe**，让 IO 与 decode 并行——本质上和 Parquet 的 `prefetchRowGroups` 一样，只是粒度换成 stripe。

### 2.5.4 Stripe 元数据与数据读取

`velox/dwio/dwrf/reader/StripeReaderBase.cpp` 的 `fetchStripe`：

```cpp
std::unique_ptr<const StripeMetadata> StripeReaderBase::fetchStripe(
    uint32_t index, bool& preload) const {
  auto stripe = fileFooter.stripes(index);
  uint64_t offset = stripe.offset();
  uint64_t length = stripe.indexLength() + stripe.dataLength() +
                    stripe.footerLength();

  std::unique_ptr<BufferedInput> stripeInput;
  if (reader_->bufferedInput().isBuffered(offset, length)) {
    preload = true;
    stripeInput = nullptr;         // 已在缓存，无需新 input
  } else {
    stripeInput = reader_->bufferedInput().clone();
    if (preload) {
      // 元数据已缓存 → 跳过 index 段
      if (cache && cache->has(StripeCacheMode::INDEX, index)) {
        offset += stripe.indexLength();
        length -= stripe.indexLength();
      }
      stripeInput->enqueue({offset, length, "stripe"});
      stripeInput->load(LogType::STRIPE);
    }
  }

  // 读 stripe footer，解密，构造 StripeMetadata
  // ...
  return std::make_unique<const StripeMetadata>(...);
}
```

**DWRF 的 IO 特点**：
- 一个 stripe 内部又分为 **index / data / footer** 三段，可以分别缓存
- `StripeMetadataCache` 专门缓存 index/footer，避免重复读
- 真正的列数据通过 `StripeStreamsImpl` 按列 lazily 读

### 2.5.5 DWRF 整体流程

```
1. 读文件尾 → PostScript → Footer (含所有 stripe 元信息)
        │
        ▼
2. 按 split/过滤选择 stripe 序列
        │
        ▼
3. DwrfRowReader::next()
        │
        ▼
4. UnitLoader::getLoadedUnit(stripeIdx)
        │   → DwrfUnit::load()
        │     → ensureDecoders()
        │         → fetchStripe()  // 读 stripe footer
        │     → loadDecoders()    // 读所有列 stream，触发 IO
        ▼
5. StripeStreamsImpl 提供 stream 接口
        │
        ▼
6. SelectiveDwrfReader::readWithVisitor 解码 + 过滤
        │
        ▼
7. 产出到 RowVector
```

---

## 2.6 Scan 算子到 Reader 的协同

### 2.6.1 TableScan：拉取入口

`velox/exec/TableScan.h`：

```cpp
class TableScan : public SourceOperator {
  RowVectorPtr getOutput() override {
    if (needNewSplit_) getSplit();
    auto result = dataSource_->next(readBatchSize_, future_);
    readBatchSize_ = calculateBatchSize(fileEstimatedRowSize_);
    return result;
  }

 private:
  std::unique_ptr<connector::DataSource> dataSource_;
  vector_size_t readBatchSize_;
  int64_t fileEstimatedRowSize_;
  ContinueFuture future_;   // 用来做 backpressure
};
```

`future_` 是**反压信号**：当 reader 无法立即返回数据（比如等 IO）时，给 scan 一个 future，scan 会把控制权交还调度器，等 future ready 再继续。这样 IO 慢的 task 不会占住 driver 线程。

### 2.6.2 HiveDataSource → FileSplitReader

`velox/connectors/hive/FileDataSource.{h}` 和 `FileSplitReader.{h}`：

```cpp
class FileDataSource : public DataSource {
  std::optional<RowVectorPtr> next(uint64_t size, ContinueFuture& future) override {
    auto rows = splitReader_->next(size, output_);
    auto passed = evaluateRemainingFilter(output_);
    runtimeStats_.rawRowCount += rows;
    return output_;
  }
};

class FileSplitReader {
  uint64_t next(uint64_t size, VectorPtr& output) {
    return baseRowReader_->next(size, output);   // 委托给 dwio RowReader
  }

 protected:
  void createReader() {
    baseReader_ = std::make_unique<ParquetReader>(
        std::move(input), baseReaderOpts_);
  }
  void createRowReader() {
    baseRowReader_ = baseReader_->createRowReader(baseRowReaderOpts_);
  }
};
```

### 2.6.3 并发控制点

整个链路上有**多个并发维度**：

| 维度 | 控制方 | 机制 |
|---|---|---|
| Query 内 driver 并发 | exec scheduler | task 的 numDrivers / 每driver 1 split |
| 单 split 内 row group / stripe 并发 | Reader (Parquet/DWRF) | prefetchRowGroups + UnitLoader 窗口 |
| 列 IO 合并 | BufferedInput | 两阶段 enqueue + load |
| 跨 reader cache 共享 | AsyncDataCache | 全局共享，文件级去重 |
| IO 提交线程池 | CachedBufferedInput.executor_ | folly::Executor 异步 prefetch |
| 反压信号 | TableScan.future_ | DataSource 给 future，scan 交还 driver |

---

## 2.7 完整 IO 提交链路

以一次 Parquet 扫描为例，从 scan 算子到磁盘 IO 的完整链路：

```
[driver thread]  exec::TableScan::getOutput()
                       │
                       │ dataSource->next(batchSize, future)
                       ▼
                 HiveDataSource::next()
                       │
                       │ splitReader->next()
                       ▼
                 FileSplitReader::next()
                       │
                       │ baseRowReader->next()
                       ▼
                 ParquetRowReader::next()
                       │
                       │ scheduleRowGroups(rowGroupIds, currentGroup, structReader)
                       ▼
                 ReaderBase::scheduleRowGroups()
                       │
                       │ for each row group in [current, current+prefetch]:
                       │    structReader.loadRowGroup(rg)
                       ▼
                 StructColumnReader::loadRowGroup(rg)
                       │
                       │ enqueueRowGroup(rg, newInput)  // 递归加所有列的 region
                       │ newInput->load(STRIPE)
                       ▼
                 CachedBufferedInput::load()
                       │
                       │ ① 分组：cache hit / SSD hit / storage miss
                       │ ② CoalescedLoad::loadOrFuture()
                       ▼
[executor thread] ─── CoalescedLoad::loadData()  ──────────────┐
                       │                                         │
                       │ readPins() → coalesceIo()               │
                       │ (合并邻近 region 为一个大 IO)             │
                       ▼                                         │
                 ReadFileInputStream::vread(regions, iobufs)     │
                       │                                         │
                       │ pread(fd, ...)                          │
                       ▼                                         │
                 Kernel VFS  (本地 or 远程协议)                   │
                                                                 │
[driver thread]  ◄─── entry->setExclusiveToShared()  ◄───────────┘
                       │
                       │ 数据已在内存，reader 继续解码
                       ▼
                 PageReader::readWithVisitor()
                       │
                       ▼
                 SelectiveColumnReader::readWithVisitor()
                       │
                       ▼
                 RowVector 产出
```

**要点总结**：

1. **Driver 线程只做调度和解码**，真正的 IO 在 executor 上异步执行；当 IO 未完成时 driver 通过 `future_` 让出。
2. **IO 合并跨三层发生**：
   - `StructColumnReader::enqueueRowGroup` 把同 row group 多列的 region 合到同一 `BufferedInput`
   - `CachedBufferedInput::load` 内部再做 cache/SSD/storage 分桶合并
   - `coalesceIo` 在 storage 读时把邻近 region 合并成一次 `vread`
3. **SSD 写入是完全异步、批量、分片并行**的，不阻塞读路径
4. **CoalescedLoad 是天然的 dedup 机制**：并发的相同请求自动等待，不重复 IO
5. **预读是自适应的**：基于 `TrackingData` 的访问密度决定是否合并、是否预读，避免对稀疏列的浪费

---

## 附录：关键文件索引

| 组件 | 路径 |
|---|---|
| AsyncDataCache | `velox/common/caching/AsyncDataCache.{h,cpp}` |
| CacheShard | `velox/common/caching/AsyncDataCache.h` (类内) |
| AsyncDataCacheEntry | `velox/common/caching/AsyncDataCache.h:156` |
| SsdCache | `velox/common/caching/SsdCache.{h,cpp}` |
| SsdFile / SsdRun | `velox/common/caching/SsdFile.{h,cpp}` |
| ScanTracker | `velox/common/caching/ScanTracker.{h,cpp}` |
| FileGroupStats | `velox/common/caching/FileGroupStats.h` |
| CoalesceIo | `velox/common/base/CoalesceIo.h` |
| Reader 抽象 | `velox/dwio/common/Reader.h` |
| InputStream | `velox/dwio/common/InputStream.h` |
| BufferedInput | `velox/dwio/common/BufferedInput.{h,cpp}` |
| CachedBufferedInput | `velox/dwio/common/CachedBufferedInput.{h,cpp}` |
| CacheInputStream | `velox/dwio/common/CacheInputStream.{h,cpp}` |
| UnitLoader | `velox/dwio/common/UnitLoader.h` |
| Parquet Reader | `velox/dwio/parquet/reader/ParquetReader.{h,cpp}` |
| Parquet PageReader | `velox/dwio/parquet/reader/PageReader.{h,cpp}` |
| DWRF Reader | `velox/dwio/dwrf/reader/DwrfReader.{h,cpp}` |
| DWRF ReaderBase | `velox/dwio/dwrf/reader/ReaderBase.{h,cpp}` |
| TableScan | `velox/exec/TableScan.h` |
| HiveDataSource | `velox/connectors/hive/FileDataSource.{h,cpp}` |
| FileSplitReader | `velox/connectors/hive/FileSplitReader.{h,cpp}` |

---

*本文档基于Velox `bh-play` 分支源码撰写，所有代码引用均带 `文件:行号` 锚点。*
