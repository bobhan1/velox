=================
内存管理
=================

背景
----------

Velox 内存系统构建在 `std::mmap <https://man7.org/linux/man-pages/man2/mmap.2.html>`_ 库之上，用来避免
std::malloc 带来的 `内存碎片问题 <https://stackoverflow.com/questions/3770457/what-is-memory-fragmentation>`_。它既为查询执行提供基础的内存分配函数，也提供公平内存共享、透明文件缓存以及服务端内存不足
（OOM）预防等高级内存管理能力。

Velox 提供大块连续和非连续缓冲区分配函数，用来优化查询内存分配模式。例如，查询可以通过 std::mmap 直接从 OS 分配物理内存，为哈希表
（*HashTable::allocateTables*）分配一大块连续内存。对于小缓冲区分配，查询可以先分配一大块非连续内存，然后在其上使用类似
*StreamArena* 或 *HashStringAllocator* 的 `memory arena 技术 <https://nullprogram.com/blog/2023/09/27/>`_ 提供小块分配，从而减少昂贵的实际内存分配次数。

Velox 通过在运行时根据内存使用变化调整各个查询的内存容量，在运行查询之间提供公平的内存共享。这个过程称为内存仲裁。它确保所有查询已分配的总内存容量处于系统配置的查询内存限制之内。同时，它也会防止单个查询超出用户配置的单查询内存限制。当查询尝试分配超过其当前容量的内存时，内存仲裁要么从容量更大的其他查询回收已使用内存来增加该查询的容量，要么在该查询超过单查询内存限制时从查询自身回收内存，在其当前容量内释放空间。内存回收通过 `disk spilling <https://facebookincubator.github.io/velox/develop/spilling.html>`_ 等技术实现。

Velox 提供透明文件缓存，通过热点数据复用和预取来加速表扫描。文件缓存与内存系统集成，从而在文件缓存和查询内存之间实现动态内存共享。当查询内存分配失败时，会通过缩小文件缓存来重试该分配。因此，文件缓存大小会根据查询内存使用变化自动调整。

Velox 通过 std::mmap 库自行管理物理内存分配，从而提供服务端内存不足（OOM）预防能力。这使得我们可以对 Velox 内存使用的
`Resident Set Size (RSS) <https://en.wikipedia.org/wiki/Resident_set_size#:~:text=In%20computing%2C%20resident%20set%20size,in%20main%20memory%20(RAM).>`_
进行显式控制。Velox 中的内存分配器处理来自文件缓存和查询内存的所有内存分配。它确保总分配内存不会超过为 Velox 配置的系统内存限制。为了进一步处理非 Velox 组件带来的尖峰内存使用，Velox 提供了通用的服务端内存反压机制：当检测到服务端处于低内存状态时，它会自动缩小文件缓存，将未使用的 Velox 内存返还给 OS。

总体设计
--------------

Velox 内存系统由以下主要内存组件组成：内存管理器（*MemoryManager*）、内存分配器（*MemoryAllocator*）、文件缓存（*AsyncDataCache*）、内存仲裁器（*MemoryArbitrator*）以及若干内存池（*MemoryPool*）。内存管理器会创建所有其他组件。它在初始化内存系统时创建内存分配器和内存仲裁器，并按需为查询执行创建内存池。

.. image:: images/memory-system.png
    :width: 600
    :align: center

当查询开始执行时，它首先从内存管理器创建一个根内存池（查询池），然后根据查询计划从根节点创建一棵子内存池树：每个查询 task 对应一个子内存池（task pool），每个 plan node 对应一个孙级内存池（node pool），每个 operator 实例对应一个曾孙级内存池（operator pool）。查询执行期间，它从叶子 operator pool 分配内存，并将内存使用量向上传播到根 query pool。如果根节点聚合后的内存使用量超过当前查询内存容量，query pool 会向内存仲裁器发送请求以增长其容量。内存仲裁器要么从系统中容量最大的其他查询回收已使用内存，从而增长请求方 query pool 的容量；要么在请求方 query pool 超过单查询内存限制或其本身就是系统中容量最大者时，从请求方 query pool 自身回收内存，以便在其当前容量内释放空间。已使用内存的回收通过 `disk spilling <https://facebookincubator.github.io/velox/develop/spilling.html>`_ 等技术实现。如果内存仲裁成功，叶子 operator pool 就可以继续从内存分配器进行实际内存分配。如果内存仲裁失败，则系统中容量最大的查询会被选择失败，并返回查询内存容量超出错误（local OOM）。失败的查询可能是请求方 query pool 本身，也可能不是。

内存分配器在自己管理的内存空间中以机器页（4KB）为单位执行实际内存分配。它跟踪已分配内存量，并在分配请求超过系统内存限制时返回错误。这使得 Velox 可以显式控制其内存使用的 RSS，从而帮助预防服务端 OOM。

当用户查询访问远程存储时，文件缓存提供内存中的热点数据缓存和预取功能。它直接从内存分配器分配内存，不计入查询内存使用量。为了避免过多文件缓存内存使用导致内存分配失败，文件缓存会通过缩小文件缓存来重试失败的分配。这就根据用户查询负载变化，在文件缓存和查询内存之间实现了动态内存共享。

.. image:: images/memory-function.png
    :width: 800
    :align: center
    :alt: 内存管理功能

总结来说，内存管理器管理内存池，并协调不同内存组件之间的访问。内存池跟踪查询的内存使用量，并与内存仲裁器交互，调整运行中查询之间的内存容量分配，以实现公平的内存共享。内存分配器管理物理内存分配以防止服务端 OOM，并与文件缓存交互，在查询内存和文件缓存之间实现动态内存共享，从而最大化内存效率。本文档其余部分会详细描述每个内存组件。

内存管理器
--------------

.. image:: images/memory-manager.png
    :width: 600
    :align: center
    :alt: 内存管理器

内存管理器在服务端启动时根据提供的 *MemoryManager::Options* 创建。它创建一个内存分配器实例，用来管理通过内存池分配的查询内存以及通过文件缓存分配的缓存内存的物理内存分配。它确保总分配内存处于系统内存限制之内（由 *MemoryManager::Options::allocatorCapacity* 指定）。内存管理器还会创建一个内存仲裁器实例，用来在运行中的查询之间仲裁内存容量。它确保已分配的查询内存总容量处于查询内存限制之内（由 *MemoryManager::Options::arbitratorCapacity* 指定）。内存仲裁器还会通过 `disk spilling <https://facebookincubator.github.io/velox/develop/spilling.html>`_ 回收过度使用的内存，防止单个查询超出其单查询内存限制（由 *QueryConfig::query_max_memory_per_node* 指定；详情参见 `内存仲裁器章节 <#memory-arbitrator>`_）。

Velox 内存系统设置完成后，内存管理器会管理用于查询执行的内存池。当查询开始时，它从内存管理器创建一个根 query pool，然后根据查询计划从 query pool 创建一棵子 pool 树（详情参见 `内存池章节 <#memory-pool>`_），用于内存分配和使用量跟踪。

内存管理器会跟踪所有存活的 query pool，以供内存仲裁过程使用。当 query pool 向内存管理器发送请求以增长其容量（*MemoryManager::growPool*）时，内存管理器会将请求连同存活 query pool 列表作为仲裁候选者转发给内存仲裁器。内存仲裁器优先从容量最大的候选者回收已使用内存，并相应地用释放出来的内存空间增加请求方 pool 的容量。如果请求方 pool 已经是所有候选者中容量最大的 pool，则内存仲裁器会从请求方自身回收内存，以便在其当前容量内释放空间。内存仲裁流程的详细说明参见 `内存仲裁流程章节 <#memory-arbitration-process>`_。

内存管理器并不拥有用户创建的 query pool，只是通过 *MemoryManager::dropPool* 方法跟踪其存活状态。该方法由 query pool 的析构函数调用，用于将自身从被跟踪列表（*MemoryManager::pools_*）中移除。*QueryCtx* 对象拥有 query pool，query pool 会一直存活到查询结束。

内存管理器会创建并拥有一个系统根 pool，用于 Velox 内部操作，例如 `disk spilling <https://facebookincubator.github.io/velox/develop/spilling.html>`_。系统根 pool 与用户创建的查询根 pool 的区别在于，系统根 pool 没有单查询内存限制，因此不参与内存仲裁。原因是系统操作不是代表某个特定用户查询执行的。以 `disk spilling <https://facebookincubator.github.io/velox/develop/spilling.html>`_ 为例，它由内存仲裁触发，用来从查询中释放已使用内存。我们不期望系统操作期间产生显著内存使用，并且最终内存分配器会保证实际分配内存处于系统内存限制之内，无论这些内存用于系统操作还是用户查询执行。在实践中，我们应该从内存分配器中预留一些空间来补偿此类系统内存使用。可以通过将查询内存限制（*MemoryManager::Options::arbitratorCapacity*）配置得小于系统内存限制（*MemoryManager::Options::allocatorCapacity*）来实现这一点（详情参见 `OOM 预防章节 <#server-oom-prevention>`_）。

内存系统初始化
^^^^^^^^^^^^^^^^^^^

下面是 Prestissimo 中初始化 Velox 内存系统的代码块：

.. code-block:: c++
  :linenos:

   void PrestoServer::initializeVeloxMemory() {
     auto* systemConfig = SystemConfig::instance();
     const uint64_t memoryGb = systemConfig->systemMemoryGb();
     MemoryManager::Options options;
     options.allocatorCapacity = memoryGb << 30;
     options.useMmapAllocator = systemConfig->useMmapAllocator();
     if (!systemConfig->memoryArbitratorKind().empty()) {
       options.arbitratorKind = systemConfig->memoryArbitratorKind();
       const uint64_t queryMemoryGb = systemConfig->queryMemoryGb();
       options.queryMemoryCapacity = queryMemoryGb << 30;
       ...
     }
     memory::initializeMemoryManager(options);

     if (systemConfig->asyncDataCacheEnabled()) {
       ...
       cache_ = std::make_shared<cache::AsyncDataCache>(
          memoryManager()->allocator(), memoryBytes, std::move(ssd));
     }
     ...
   }

* L5：从 Prestissimo 系统配置设置内存分配器容量（系统内存限制）
* L6：从 Prestissimo 系统配置设置内存分配器类型。如果 *useMmapAllocator* 为 true，则使用 *MmapAllocator*，否则使用 *MallocAllocator*。`内存分配器章节 <#memory-allocator>`_ 会描述这两类分配器
* L8：从 Prestissimo 系统配置设置内存仲裁器类型。目前只支持 *“SHARED”* 仲裁器类型（参见 `内存仲裁器章节 <#memory-arbitrator>`_）。*“NOOP”* 仲裁器类型很快会被废弃（`#8220 <https://github.com/facebookincubator/velox/issues/8220>`_）
* L10：从 Prestissimo 系统配置设置内存仲裁器容量（查询内存限制）
* L13：创建进程级内存管理器，该内存管理器会基于前面步骤初始化的 MemoryManager::Options 在内部创建内存分配器和仲裁器
* L15-19：如果 Prestissimo 系统配置启用了文件缓存，则创建文件缓存

内存池
-----------

内存池为查询执行提供内存分配函数。它还会跟踪查询的内存使用量，以执行单查询内存限制。如“查询内存池层级”图所示，查询会创建一棵与查询计划镜像对应的内存池树，以便细粒度跟踪内存使用量，找出哪些 task 或 operator 使用了最多内存。在树根处，*QueryCtx* 从内存管理器创建一个根 query pool。每个查询 task 从 query pool 创建一个子 task pool。查询 task 执行查询计划的一个片段（例如 Prestissimo 分布式查询执行计划中的一个执行阶段）。task 计划片段中的每个 plan node 从 task pool 创建一个子 node pool（*Task::getOrAddNodePool*）。每个 plan node 属于一个或多个 task 执行 pipeline。每个 pipeline 可能有多个 driver 实例并行运行。每个 driver 实例由一条查询 operator pipeline 组成，而 operator 是 driver 中某个查询 plan node 的实例。因此，每个 operator 会从 node pool 创建一个子 operator pool（*Task::addOperatorPool*）。

.. image:: images/memory-pool.png
    :width: 500
    :align: center
    :alt: 内存池

查询从树叶处的 operator pool 分配内存，并将内存使用量一路向上传播到树根处的 query pool，以检查内存使用量是否超过单查询内存限制。内存分配始终发生在叶子 operator pool，中间 pool 只聚合内存使用量（node pool 和 task pool），由根 query pool 执行单查询内存限制。基于这一点，我们引入了两种内存池类型（由 *MemoryPool::Kind* 定义）来简化内存池管理：一种是 *LEAF* 类型，只允许执行内存分配；另一种是 *AGGREGATE* 类型，用来聚合所有子 pool 的内存使用量，但不允许直接分配内存。因此，operator pool 是 *LEAF* 类型，其余所有 pool 都是 *AGGREGATE* 类型。我们只在根 query pool 执行内存限制检查。

内存使用跟踪
^^^^^^^^^^^^^^^^^^^^^

为了跟踪查询内存使用量，叶子 operator pool 需要为每次分配将内存使用量一路传播到根 query pool 并检查内存限制，但这样会很慢。因此，内存池使用内存 reservation 机制跟踪查询内存使用量。reservation 会以 1MB 或更大的块进行，以避免每次单独分配都产生过多加锁、传播和内存使用检查（见下面对 *MemoryPool::quantizedSize* 的描述）。叶子 operator pool 维护两个用于内存 reservation 的计数器：一个是实际已使用内存（*MemoryPoolImpl::usedReservationBytes_*），另一个是从根 query pool 预留的内存（*MemoryPoolImpl::reservationBytes_*）。两个计数器的差值就是该叶子 operator pool 可用的内存。

中间 pool 只使用 *reservationBytes_* 统计其所有子 pool 持有的聚合内存 reservation。根 query pool 有两个额外的计数器用于内存限制检查：一个是当前内存容量（*MemoryPoolImpl::capacity_*），也就是查询可使用的内存量。内存仲裁器根据运行中的查询数量、总查询内存限制以及每个查询需要的内存量来设置该值。另一个是最大容量（*MemoryPool::maxCapacity_*），也就是查询最多可以增长到的容量。它由用户设置，并在查询生命周期内固定不变（*QueryConfig::kQueryMaxMemoryPerNode*）。内存仲裁器不能将查询的 *capacity_* 设置为超过其 *maxCapacity_* 限制。

当根 query pool 收到新的内存 reservation 请求时，它会增加 *reservationBytes_* 并检查其是否处于当前 *capacity_* 限制之内。如果处于限制内，根 query pool 接受该请求。否则，根 query pool 会通过内存管理器请求内存仲裁器增长其容量（详情参见 `内存仲裁器章节 <#memory-arbitrator>`_）。如果内存仲裁失败，根 query pool 会以查询内存容量超出错误（local OOM 错误）让请求失败。

*MemoryPool::reserve* 和 *MemoryPool::release* 是内存池用于内存 reservation 的两个方法。内存 reservation 是线程安全的，*MemoryPool::reserveThreadSafe* 是实现内存 reservation 逻辑的主函数：

#. 叶子内存池调用 *MemoryPool::reservationSizeLocked* 计算新的所需 reservation（*incrementBytes*）。该值基于内存分配大小和可用内存 reservation（*reservationBytes_ -  usedReservationBytes_*）。

#. 如果 *incrementBytes* 为零，说明叶子内存池有足够可用 reservation，因此不需要新的 reservation，只需更新 *usedReservationBytes_* 以反映新的内存使用量。

#. 如果 *incrementBytes* 非零，叶子内存池需要调用 *MemoryPool::incrementReservationThreadSafe*（见下文），将增量一路传播到根内存池，以检查新的 reservation 请求是否超过查询当前容量。如果未超过，则通过相应增加 *reservationBytes_* 来接受该 reservation。

   注意，如果 *MemoryPool::incrementReservationThreadSafe* 失败，它会抛出异常，并以 local OOM 错误使内存分配请求失败。

#. reservation 成功后，叶子内存池回到步骤 1，检查是否已有足够可用 reservation 来满足分配请求。

   注意，对同一个叶子内存池的并发分配请求可能会抢走步骤 3 中获得的 reservation，因此必须再次检查。从根内存池进行 reservation 时，我们不会持有叶子内存池的锁；如果涉及内存仲裁，这可能是一个阻塞操作。因此，如果同一个叶子内存池有两个并发内存 reservation 请求，可能会存在竞态条件。不过在实践中我们不期望这种情况经常发生。

如上所述，为了避免对根内存池进行频繁的并发内存 reservation 并降低 CPU 开销，叶子内存池会进行量化内存 reservation。它会将实际 reservation 字节数向上取整到下一个更大的量化 reservation 值（MemoryPool::quantizedSize）：

- 如果 size < 16MB，向上取整到下一个 1MB
- 如果 size < 64MB，向上取整到下一个 4MB
- 如果 size >= 64MB，向上取整到下一个 8MB

使用量化 reservation 后，我们永远不会预留少于 1MB 的内存。即使只需要 1KB，也必须预留 1MB；如果没有足够可用内存，查询会失败。这也意味着，如果并发度为 15，每个 driver 线程至少会预留 1MB，因此即使查询只使用几 KB，它也至少需要 15MB 内存。

MemoryPool::incrementReservationThreadSafe 的实现：

#. 非根内存池递归调用其父 pool 的 *incrementReservationThreadSafe* 方法，将 reservation 请求一路传播到根内存池。

#. 检查父 pool 的 *MemoryPool::incrementReservationThreadSafe* 结果：

   a. 如果函数返回 true，说明已从根内存池成功获得 reservation，并继续接受该 reservation（步骤 3）
   b. 如果函数返回 false，说明 reservation 成功，但根内存池检测到与其他并发 reservation 请求冲突。需要通过向 *MemoryPoolImpl::reserveThreadSafe* 返回 false，从叶子内存池重新重试
   c. 如果内存 reservation 在根内存池失败，函数期望抛出查询内存容量超出异常，内存分配失败

#. 调用 *MemoryPool::maybeIncrementReservation* 尝试增加 reservation 并检查结果：

   a. 对于非根内存池，这应该总是成功，因为我们只在根内存池检查容量
   b. 对于根内存池，如果 reservation 请求超过当前容量，该函数可能返回 false，并进入步骤 4 请求内存仲裁

#. 根内存池调用 *MemoryManager::growPool* 来增长其容量。这会触发内存仲裁器内部的内存仲裁流程。

#. 如果 *MemoryManager::growPool* 返回 true，说明内存容量增长成功（或在当前容量内降低了内存使用量）。函数会再次调用 *MemoryPool::maybeIncrementReservation*，检查是否可以满足内存 reservation。如果不能满足，则说明存在并发内存 reservation 请求抢走了增长得到的内存容量。这种情况下返回 false，从叶子内存池重新重试（步骤 2-b）。否则返回 true（步骤 2-a）。

#. 如果 *MemoryManager::growPool* 返回 false，说明无法通过内存仲裁器增长容量，于是抛出查询内存容量超出错误（步骤 2-c）。

内存池 API
^^^^^^^^^^^^^^^^

内存池有三组 API，分别用于内存池管理、内存分配和内存仲裁。下面列出每组中主要使用的 API。

内存池管理
""""""""""""""""""""""

.. code-block:: c++

  /// 创建具有指定 'name' 和 'maxCapacity' 的根内存池。
  /// 'reclaimer' 用于内存仲裁流程。
  std::shared_ptr<MemoryPool> MemoryManager::addRootPool(
     const std::string& name = "",
     int64_t maxCapacity = kMaxMemory,
     std::unique_ptr<MemoryReclaimer> reclaimer = nullptr);

  /// 创建 aggregate 子内存池。该 pool 允许继续从中创建子内存池，
  /// 并用于聚合其子 pool 的内存使用量。
  /// aggregate 内存池不允许直接分配内存。
  virtual std::shared_ptr<MemoryPool> MemoryPool::addAggregateChild(
     const std::string& name);

  /// 创建 leaf 子内存池。该 pool 允许分配内存，但不允许创建子 pool。
  virtual std::shared_ptr<MemoryPool> MemoryPool::addLeafChild(
     const std::string& name);

  /// 为 operator 创建新的 MemoryPool 实例，将其存储在 task 中以保证生命周期，
  /// 并返回裸指针。
  velox::memory::MemoryPool* Task::addOperatorPool(
     const core::PlanNodeId& planNodeId,
     int pipelineId,
     uint32_t driverId,
     const std::string& operatorType);

  /// 为 plan node 创建新的 MemoryPool 实例，将其存储在 task 中以保证生命周期，
  /// 并返回裸指针。
  memory::MemoryPool* Task::getOrAddNodePool(
     const core::PlanNodeId& planNodeId);

内存分配
"""""""""""""""""

内存池提供三类内存分配。如果用户需要分配一大块缓冲区，并且该缓冲区不要求连续，可以使用 *MemoryPool::allocateNonContiguous* 分配多个可变大小的缓冲区（详情参见 `非连续分配章节 <#non-contiguous-allocation>`_）。Velox 将这种分配用于 *RowContainer*、*StreamArena* / *HashStringAllocator*、*AsyncDataCache* 等。如果用户需要分配大小 > 1MB 的大块连续缓冲区，可以使用 *MemoryPool::allocateContiguous* 通过 std::mmap 直接从 OS 分配一大块物理内存（详情参见 `连续分配章节 <#contiguous-allocation>`_）。Velox 将这种分配用于 *HashTable*。对于其他临时分配，可以使用 *MemoryPool::allocate*。内存分配器会根据实际分配大小决定如何分配内存（详情参见 `小块分配章节 <#small-allocation>`_）。

.. code-block:: c++

  /// 分配指定 'size' 的缓冲区。如果内存分配小于预定义阈值，
  /// 则将分配委托给 std::malloc（MmapAllocator::Options::maxMallocBytes）。
  virtual void* MemoryPool::allocate(int64_t size) = 0;

  /// 释放一个已分配的缓冲区。
  virtual void MemoryPool::free(void* p, int64_t size) = 0;

  /// 分配一个或多个 run，其总和至少为 'numPages'，
  /// 最小 run 至少为 'minSizeClass' 页。'minSizeClass' 必须
  /// <= 最大 size class 的大小（size class 定义见非连续分配章节）。
  /// 成功时，新内存在 'out' 中返回，'out' 之前引用的所有内存会被释放。
  /// 如果分配失败则抛出异常，'out' 不引用任何内存，部分成功分配的内存也会被释放。
  virtual void MemoryPool::allocateNonContiguous(
     MachinePageCount numPages,
     Allocation& out,
     MachinePageCount minSizeClass = 0) = 0;

  /// 释放非连续 'allocation'。返回时 'allocation' 为空。
  virtual void MemoryPool::freeNonContiguous(Allocation& allocation) = 0;

  /// 对 'numPages' 执行一次大块连续 mmap。成功时，新映射页在 'out' 中返回。
  /// 即使分配失败，'out' 之前引用的任何已映射页都会在所有情况下解除映射。
  virtual void MemoryPool::allocateContiguous(
     MachinePageCount numPages,
     ContiguousAllocation& out) = 0;

  /// 释放连续 'allocation'。返回时 'allocation' 为空。
  virtual void MemoryPool::freeContiguous(ContiguousAllocation& allocation) = 0;

内存仲裁
""""""""""""""""""

下面的 `内存仲裁器章节 <#memory-arbitrator>`_ 会讨论这些与内存仲裁相关的方法如何在内存仲裁和回收流程中使用。

.. code-block:: c++

  /// 返回尚未预留给使用方、并且可以通过降低该内存池限制释放的字节数。
  virtual uint64_t MemoryPool::freeBytes() const = 0;

  /// 将内存池容量增加 'bytes'。函数返回增长后的内存池新容量。
  virtual uint64_t MemoryPool::grow(uint64_t bytes) = 0;

  /// 通过降低该内存池容量释放最多指定数量的未使用内存 reservation，
  /// 但不会实际释放任何已使用内存。函数返回实际释放的内存字节数。
  /// 如果 'targetBytes' 为零，函数会释放所有未使用的内存 reservation 字节。
  virtual uint64_t MemoryPool::shrink(uint64_t targetBytes = 0) = 0;

  /// 由内存仲裁器调用，用于进入内存仲裁处理。
  /// 如果未设置 'reclaimer_'，则为空操作，否则调用 reclaimer 的对应方法。
  virtual void MemoryPool::enterArbitration();

  /// 由内存仲裁器调用，用于离开内存仲裁处理。
  /// 如果未设置 'reclaimer_'，则为空操作，否则调用 reclaimer 的对应方法。
  virtual void MemoryPool::leaveArbitration();

  /// 该函数估算可回收字节数，并返回到 'reclaimableBytes'。
  /// 如果未设置 'reclaimer'，函数返回 std::nullopt。
  /// 否则会调用 reclaimer 的对应方法。
  virtual std::optional<uint64_t> reclaimableBytes() const = 0;

  /// 由内存仲裁器调用，用于按指定目标回收字节数从该内存池回收内存。
  /// 如果 'targetBytes' 为零，则尝试从该内存池回收所有可回收内存。
  /// 如果未设置 reclaimer，则为空操作，否则调用 reclaimer 的对应方法。
  virtual uint64_t MemoryPool::reclaim(uint64_t targetBytes);

内存仲裁器
-----------------

内存仲裁器用于在运行中的查询之间仲裁内存容量，以实现公平内存共享并防止查询超出其内存限制。为了在运行查询之间仲裁内存容量，内存仲裁器需要能够通过 `disk spilling <https://facebookincubator.github.io/velox/develop/spilling.html>`_ 等技术从查询中回收已使用内存，然后通过相应调整内存池容量，在查询之间转移释放出来的内存（详情参见 `内存仲裁流程章节 <#memory-arbitration-process>`_）。

*MemoryArbitrator* 被定义为支持不同查询系统的不同实现。目前，我们为 Prestissimo 和 Prestissimo-on-Spark 都实现了 *SharedArbitrator*。`Gluten <https://github.com/apache/gluten>`_ 实现了自己的内存仲裁器，用于与 `Spark memory system <https://www.linkedin.com/pulse/apache-spark-memory-management-deep-dive-deepak-rajak/>`_ 集成。*SharedArbitrator* 确保已分配内存容量总量处于查询内存限制（*MemoryManager::Options::arbitratorCapacity*）之内，并确保每个单独查询的容量处于单查询内存限制（*MemoryPool::maxCapacity_*）之内。当查询需要增长其容量时，*SharedArbitrator* 要么在该查询已超过最大内存容量时从查询自身回收已使用内存，要么从系统中内存容量最大的其他查询回收已使用内存来增加它的容量。

内存仲裁流程
^^^^^^^^^^^^^^^^^^^^^^^^^^

.. image:: images/memory-arbitration.png
    :width: 800
    :align: center
    :alt: 内存仲裁流程

*SharedArbitrator* 中端到端的内存仲裁流程如下：

#. 查询 operator A 从其叶子 operator pool（operator pool A）分配内存
#. operator pool A 向根 query pool（query pool A）发送内存 reservation 请求
#. query pool A 是根内存池，它会检查内存 reservation 请求是否处于当前容量（*MemoryPoolImpl::capacity_*）之内。这里假设请求已超过当前容量，从而触发内存仲裁
#. query pool A 向内存管理器发送请求，为新的 reservation 增长其容量（*MemoryManager::growPool*）
#. 内存管理器将请求转发给内存仲裁器（*MemoryArbitrator::growCapacity*），同时传递请求方内存池和根 query pool 列表作为内存仲裁候选者。内存管理器会在内存仲裁过程中保持候选 query pool 存活
#. 内存仲裁器会串行化内存仲裁处理，一次处理一个请求，以确保查询之间分配的内存容量视图一致。内存仲裁器可能会收到来自不同查询，甚至来自同一查询不同 driver 线程的并发仲裁请求。对于每个内存仲裁请求：

   a. 内存仲裁器在开始内存仲裁之前调用请求方内存池的 *MemoryPool::enterArbitration* 方法。这里的请求内存池是发起内存 reservation 请求的 operator pool A。它调用关联 operator reclaimer（*Operator::MemoryReclaimer*）的 *MemoryReclaimer::enterArbitration* 方法。operator reclaimer 会将 driver 线程置入挂起状态（*Task::enterSuspended*）。为了从查询 task 回收内存，需要先暂停该 task，停止其所有 driver 线程，避免在内存回收期间并发更新其 operator 状态。如果请求内存池所属的查询 task 被选中进行内存回收，则必须将它的 driver 线程置入挂起状态；否则，由于请求 driver 线程正处于内存仲裁流程中，该查询 task 永远不会被暂停。注意，在 task pause 处理中，挂起的 driver 线程不计为 running。

   b. 内存仲裁器调用 *SharedArbitrator::ensureCapacity*，检查加上新的 reservation 后，请求方 query pool 是否超过其最大内存容量限制（*MemoryPool::maxCapacity_*）。如果没有超过，则进入步骤 6-c。否则，内存仲裁器会尝试从请求方 pool 自身回收已使用内存。如果内存回收释放了足够内存，使请求方 pool 可以在其当前容量内满足新的 reservation，则内存仲裁成功。如果请求方 pool 仍然超过最大内存容量限制，则内存仲裁失败。否则进入步骤 6-c。

   c. 内存仲裁器运行快路径（*SharedArbitrator::reclaimFreeMemoryFromCandidates*），从候选 query pool 回收未使用的内存 reservation，而不实际释放已使用内存。它首先尝试从自身回收，然后从具有最多空闲容量（*MemoryPool::freeBytes*）的候选 pool 回收，直到达到内存回收目标。

   d. 如果内存仲裁器在快路径上没有回收足够的空闲内存，它会运行慢路径（*SharedArbitrator::reclaimUsedMemoryFromCandidates*），从具有最多可回收内存的候选 pool 回收已使用内存（查询内部的详细内存回收流程参见 `内存回收流程章节 <#memory-reclaim-process>`_）。

   e. 如果内存仲裁器已回收足够内存，它会通过增加请求方 pool 的内存容量（*MemoryPool::grow*）将回收的内存授予请求方 pool。如果没有回收足够内存，内存仲裁器必须调用 *SharedArbitrator::handleOOM*，向容量最大的候选内存池发送内存池 abort（*MemoryPool::abort*）请求，将其作为 victim 释放内存，以便其他拥有足够内存的运行中查询继续执行。内存池 abort 会使查询执行失败，并等待其完成以释放所有持有的内存资源。

   f. 如果 victim query pool 就是请求方 pool 自身，则内存仲裁失败。否则，回到步骤 6-c，再重试一次内存仲裁后再放弃。

   g. 内存仲裁结束时，内存仲裁器调用请求方内存池的 *MemoryPool::leaveArbitration* 方法。operator reclaimer 会将其 driver 线程移出挂起状态（*Task::leaveSuspended*）。

内存回收流程
^^^^^^^^^^^^^^^^^^^^^^

下面是查询内部的内存回收流程：

#. 内存仲裁器以字节为单位的回收目标调用候选 query pool 的 *MemoryPool::reclaim* 方法，该方法会调用关联内存 reclaimer 对象的对应方法（*MemoryReclaimer::reclaim*）。query pool 使用默认实现：根据可回收字节数（*MemoryPool::reclaimableBytes*）对其子 task pool 排序，并从可回收字节数最多的 task 开始回收，直到达到回收目标。

#. query pool 调用 task pool 的 reclaim 方法，后者再调用关联的 task reclaimer（*Task::MemoryReclaimer*）。task reclaimer 首先暂停 task 执行（*Task::requestPause*），然后根据可回收字节数对其子 node pool 排序，并从可回收字节数最多的 node pool 回收内存。在达到回收目标或已从所有 node pool 回收之后，task reclaimer 恢复 task 执行（*Task::resume*）。

#. task pool 调用 node pool 的 reclaim 方法，node pool 会从其子 operator pool 中可回收字节数最多的 pool 回收内存。

#. node pool 最终调用 operator pool 执行实际内存回收（*Operator::MemoryReclaimer*）。目前我们支持通过 disk spilling 和 table writer flush 进行内存回收。*Operator::reclaim* 是为支持内存回收而添加的，其默认实现不做任何事。只有可 spill 的 operator 会覆盖该方法：*OrderBy*、*HashBuild*、*HashAggregation*、*RowNumber*、*TopNRowNumber*、*MarkDistinct*、*Window* 和 *TableWriter*。目前，我们会简单地将可 spill operator 的 row container 中的全部内容 spill 出去以释放内存。等为 row container 增加内存 compaction 支持后，我们可以利用 Velox 中的细粒度 disk spilling 功能，只 spill 并释放所需数量的内存。

注意，如果可 spill operator 在数据处理中途触发了内存仲裁，即使它已经停止了查询 task 执行，内存仲裁器也不能从它回收内存。为防止这种情况，我们增加了 *Operator::nonReclaimableSection_*，用于表示 operator 是否处于不可回收区间。内存仲裁器不能从处于不可回收区间的 operator 回收内存。driver 执行框架默认将正在运行的 operator 设置在不可回收区间中。可 spill operator 会选择在特定调用点清除不可回收区间，例如在实际数据处理之前进行内存 reservation（*MemoryPool::maybeReserve*）时，以允许内存仲裁器回收内存。

内存分配器
----------------

内存分配器管理通过内存池分配的查询内存以及直接从文件缓存分配的缓存内存的物理内存分配。内存分配器确保总分配内存始终处于系统内存限制之内。*MemoryAllocator* 定义了内存分配器接口。我们有两个分配器实现：*MallocAllocator* 将所有内存分配委托给 std::malloc，简单且可靠。我们将其作为默认选项，但认为它存在内存碎片导致 RSS 变化的问题。因此我们构建了 *MMapAllocator*，使用 std::mmap 管理物理内存分配，以便显式控制 RSS。我们尚未确认 *MmapAllocator* 是否比 *MallocAllocator* 更好，但已经能够用它运行规模可观的 Prestissimo 工作负载。未来我们会用这两个分配器比较该工作负载，以确定哪个更好。用户可以通过设置 *MemoryManager::Options::useMmapAllocator* 为其应用选择分配器（示例参见 `内存系统初始化章节 <#memory-system-setup>`_）。

非连续分配
^^^^^^^^^^^^^^^^^^^^^^^^^

.. image:: images/size-class.png
    :width: 500
    :align: center
    :alt: Size Class

非连续分配被定义为一个 *Allocation* 对象，它由若干 PageRun 组成。每个 page run 包含一个连续缓冲区，而不同 page run 的缓冲区不需要彼此连续。*MMapAllocator* 定义了 *MmapAllocator::SizeClass* 数据结构（类似 `Umbra <https://db.in.tum.de/~freitag/papers/p29-neumann-cidr20.pdf>`_ 中使用的结构）来管理非连续分配。一个 *SizeClass* 对象提供固定大小缓冲区（class page）的分配，class page 大小是机器页大小的 2 的幂。*MMapAllocator* 创建 9 个不同的 *SizeClass* 对象，其 class page 大小范围从 1 个机器页（4KB）到 256 个机器页（1MB）。为了分配大量机器页，*MmapAllocator* 调用 *MemoryAllocator::allocationSize* 构建分配计划（*MemoryAllocator::SizeMix*），其中包含选中的 *SizeClass* 对象列表以及从每个对象分配的 class page 数量。

*MemoryAllocator::allocationSize* 通过从最大匹配 *SizeClass* 搜索到用户指定的最小 *SizeClass* 来生成分配计划。如果最小 *SizeClass* 不是 1，最后分配的 class page 中可能会有内存浪费。如图中示例，对于 150 页的分配请求和最小 *SizeClass* 为 4 的情况，我们选择从 *SizeClass/64* 分配 2 个 class page，从 *SizeClass/16* 分配 1 个，从 *SizeClass/4* 分配 2 个。分配的机器页总数为 152。最后一个来自 *SizeClass/4* 的 class page 中浪费了两个机器页。内存分配器根据分配计划从每个选中的 *SizeClass* 对象分配内存。分配结果返回在一个 *Allocation* 对象中，其中包含 4 个 page run：两个来自 *SizeClass/64* 的 run（两个分配的 class page 在内存中不连续），一个来自 *SizeClass/16* 的 run，以及一个来自 *SizeClass/4* 的 run（两个分配的 class page 在内存中连续）。

每个 *SizeClass* 对象使用 std::mmap 设置自己的内存空间，其大小与系统内存限制相同。设置该内存空间并不会导致 OS 发生任何内存分配（也没有 backing memory），直到用户写入已分配的内存空间。SizeClass 对象将自己的内存空间划分成若干 class page，并使用 *SizeClass::pageAllocated_* bitmap 跟踪 class page 是否已分配。它使用另一个 bitmap *SizeClass::pageMapped_* 跟踪 class page 是否具有 backing memory（mapped class page）。为了确保 Velox 内存使用的 RSS 处于系统内存限制之内，我们假设已分配的 class page 总是具有 backing memory，而已释放的 class page 在调用 std::madvise 将其返还给 OS 之前也仍具有 backing memory。释放 class page 时，我们只是清除 *pageAllocated_* bitmap 中的分配位，但不会立即调用 std::madvise 释放 backing memory，因为 std::madvise 是昂贵的 OS 调用。我们也预期已释放的 class page 很可能很快再次被复用。因此，只有当 mapped class page 总数达到系统内存限制时，我们才会为了新分配移除已释放 class page 的 backing memory。*numMappedFreePages_* 用来跟踪每个 *SizeClass* 对象中仍具有 backing memory 的已释放 class page 数量。*SizeClass::adviseAway* 实现了 lazy backing memory 释放控制逻辑。

我们应用了两个优化来加速空闲 class page 查找。一个是使用聚合 bitmap（*mappedFreeLookup_*）按组跟踪空闲 class page。*mappedFreeLookup_* 中的每一位对应 *pageAllocated_* 中的 512 位（8 个 word）。如果 *mappedFreeLookup_* 中某一位被置位，则 *pageAllocated_* 中对应的 512 位里至少有一位未置位。另一个优化是使用 SIMD 指令操作 bitmap，以进一步加速 CPU 执行。

简化后的 *MmapAllocator::allocateNonContiguous* 实现如下：

.. code-block:: c++

  bool MmapAllocator::allocateNonContiguous(
      MachinePageCount numPages,
      Allocation& out,
      ReservationCallback reservationCB,
      MachinePageCount minSizeClass) override;

#. 使用 *numPages* 和 *minSizeClass* 调用 *MemoryAllocator::allocationSize*。*numPages* 指定要分配的机器页数量。*minSizeClass* 指定要从中分配的最小 class page 大小。函数在 *MemoryAllocator::SizeMix* 中返回从每个选中的 *SizeClass* 分配的 class page 数量。从所有 *SizeClass* 对象分配的机器页总和不应少于请求的 *numPages*。

#. 增加内存分配器的内存使用量，并检查是否超过系统内存限制（*MemoryAllocator::capacity_*）。如果超过，则分配失败，并回滚内存使用更新。否则，进入步骤 3 在内存池中进行 reservation。

   * *MMapAllocator* 使用 *MallocAllocator::numAllocated_* 以机器页为单位统计已分配内存
   * *MMapAllocator* 分配会被 *AsyncDataCache::makeSpace* 包装；后者在放弃前会多次通过缩小文件缓存来重试分配失败。每次重试都有 backoff delay，并使从缓存中驱逐更困难
   * *AsyncDataCache::makeSpace* 不仅会重试来自内存池的分配，也会从文件缓存自身重试分配。后一种情况下，旧缓存条目会被驱逐以便为新的缓存数据腾出空间

#. 调用 *reservationCB* 增加内存池的 reservation，以检查新的分配是否超过查询内存限制。如果超过，则回滚步骤 2 中进行的内存使用更新，并重新抛出从 *reservationCB* 捕获到的查询内存容量超出异常。如果分配来自文件缓存，则 *reservationCB* 为 null。

#. 从每个选中的 SizeClass 对象分配 class page。如果任意一个 *SizeClass* 分配失败，则整个分配失败。我们会释放已经成功的 *SizeClass* 分配，并回滚内存池 reservation（步骤 3）和内存使用（步骤 2）更新。

#. class page 分配会返回需要设置 backing memory 的机器页数量。这指的是那些已分配但还没有 backing memory、且 *SizeClass::pageMapped_* 中对应 bit 未置位的 class page。我们调用 *MmapAllocator::ensureEnoughMappedPages*，确保本次新分配后具有 backing memory 的 mapped class page 总数不超过系统内存限制。如果超过，则调用 *MmapAllocator::adviseAway* 移除已释放 class page 的 backing memory。如果 *MmapAllocator::adviseAway* 调用失败，则本次分配失败，并回滚此前步骤对本次分配所做的所有更改。

#. 调用 *MmapAllocator::markAllMapped* 将所有已分配 class page 在 *SizeClass::pageMapped_* 中标记为 mapped，分配成功。

连续分配
^^^^^^^^^^^^^^^^^^^^^

.. code-block:: c++

  virtual bool MemoryAllocator::allocateContiguous(
     MachinePageCount numPages,
     Allocation* collateral,
     ContiguousAllocation& allocation,
     ReservationCallback reservationCB = nullptr) = 0;

连续分配被定义为一个 *ContiguousAllocation* 对象，其中包含一大块连续缓冲区。它用于非常大的连续缓冲区分配（>1MB），例如分配哈希表。其实现非常简单：调用 std::mmap 直接从 OS 分配一大块连续物理内存。与非连续分配类似，它需要调用 *MmapAllocator::ensureEnoughMappedPages*，确保 mapped 内存空间大小处于系统内存限制之内。释放连续分配时，内存分配器会调用 std::munmap 立即将物理内存返还给 OS。

小块分配
^^^^^^^^^^^^^^^^

.. code-block:: c++

  void* MmapAllocator::allocateBytes(
     uint64_t bytes,
     uint16_t alignment = kMinAlignment) override;

*MmapAllocator::allocateBytes* 会根据实际分配大小（字节）以三种不同方式分配内存。如果分配大小小于配置的阈值（*MmapAllocator::Options::maxMallocBytes*），*MmapAllocator* 会将分配委托给 std::malloc。如果分配大小处于 class page 大小范围内（<= 1MB），它会从某个 *SizeClass* 对象中以 class page 的形式分配该缓冲区。否则，它会将该缓冲区作为大块连续分配来分配。

我们不期望使用 *MmapAllocator* 的查询系统产生大量小内存分配。在 Prestissimo 中，只有极少量小内存分配会委托给 std::malloc。大型内存状态，例如 *RowContainer* 和 *HashTable*，会使用连续或非连续分配。目前，我们没有对 *MmapAllocator* 中委托给 std::malloc 的内存分配设置上限。我们提供了一个选项（*MmapAllocator::Options::smallAllocationReservePct*），让查询系统可以在 *MmapAllocator* 中预留少量内存容量，以在实践中补偿这些临时小块分配。

自定义内存资源
-----------------------

*CustomMemoryResource* 允许扩展在默认 CPU 层级之外并列暴露非主机 DRAM 的内存层级，例如 GPU device memory、CXL-attached memory、pinned host memory、NUMA-bound pools。一个 resource 会绑定一个 tag、一个 allocator、一个 arbitrator，以及一个用于构建每查询 reclaimer 的 factory。构造函数要求 tag 非空，allocator、arbitrator 和 reclaimerFactory 非 null；resource 一经构造即不可变：

.. code-block:: c++

  class CustomMemoryResource {
   public:
    CustomMemoryResource(
        std::string tag,
        std::shared_ptr<MemoryAllocator> allocator,
        std::shared_ptr<MemoryArbitrator> arbitrator,
        std::function<std::unique_ptr<MemoryReclaimer>()> reclaimerFactory,
        int64_t maxCapacity = std::numeric_limits<int64_t>::max());

    const std::string& tag() const;
    int64_t maxCapacity() const;
    MemoryAllocator* allocator() const;
    MemoryArbitrator* arbitrator() const;
    std::unique_ptr<MemoryReclaimer> newReclaimer() const;
  };

每个 resource 都携带自己的 allocator 和 arbitrator；*MemoryManager* 上的默认 CPU allocator 和 arbitrator 不会与自定义 resource 共享。每个层级的 accounting 和容量决策保持自包含。当不同层级在大小、延迟、对齐方式或分配失败模式上有所不同时，这一点很重要。

注册
^^^^^^^^^^^^

resource 由 *CustomMemoryResourceRegistry* 跟踪。该类通过 *global()* 暴露进程全局 root，并通过 *createRegistry(parent)* 创建子作用域。注册和查找直接通过 *Registry* 实例完成（其 *insert*、*find* 和 *clear* 方法）；每个作用域由自己的锁保护。扩展会在进程启动时、*initializeMemoryManager* 之后，将 resource 注册到全局作用域：

.. code-block:: c++

  auto resource = std::make_shared<memory::CustomMemoryResource>(
      "device",
      std::make_shared<MyTieredAllocator>(...),
      MemoryArbitrator::create(...),
      []() { return std::make_unique<MyReclaimer>(); },
      deviceCapacity);
  memory::CustomMemoryResourceRegistry::global().insert(
      resource->tag(), resource);

对于每个想使用已注册 resource 的查询，调用方会按 tag 查找该 resource，通过 *MemoryManager::addCustomRootPool* 物化该 resource 对应的根 pool，并通过 *Builder::customPool* 将该 pool 交给 *QueryCtx*：

.. code-block:: c++

  auto* manager = memory::memoryManager();
  auto resource =
      memory::CustomMemoryResourceRegistry::global().find("device");
  VELOX_USER_CHECK_NOT_NULL(resource, "Unknown resource tag: device");
  auto devicePool = manager->addCustomRootPool("query.q0.device", resource);
  auto queryCtx = core::QueryCtx::Builder()
                      .customPool("device", std::move(devicePool))
                      .queryId("q0")
                      .build();

*addCustomRootPool* 调用 *resource->newReclaimer()* 构建该 pool 的 reclaimer，使用 *resource->maxCapacity()* 作为 pool 容量，并用 *resource->allocator()* 和 *resource->arbitrator()* 支撑该 pool。根 pool 会按 tag 作为 key 暴露在 *QueryCtx* 上：

.. code-block:: c++

  // 返回给定 resource tag 对应的自定义根 pool；如果没有则返回 nullptr。
  std::shared_ptr<memory::MemoryPool> QueryCtx::customPool(
      const std::string& tag) const;

每查询内存池层级
^^^^^^^^^^^^^^^^^^^^^^^^

对于每个通过 *Builder::customPool* 注册到 *QueryCtx* 的自定义根 pool，*Task* 会在其下构建一棵并行的 ``task → node → operator`` aggregate/leaf 子树，镜像默认层级。自定义根下的 aggregate 子节点会与其默认对应节点在同一时刻创建。这些 aggregate 的 reclaimer 来自每个 resource 的 ``reclaimerFactory``，通过 *CustomMemoryResource::newReclaimer* 创建。因此，自定义子树上的容量决策和回收会端到端由该 resource 自己的 arbitrator 和 reclaimer 管理，并与默认 DRAM 层级分离。

服务端 OOM 预防
---------------------

内存分配器确保来自 Velox 的所有内存使用都不会超过系统内存限制。由于我们预期 Velox 在运行中会使用服务端内存的很大一部分，这对于防止服务端内存耗尽至关重要。例如，Meta 的 Prestissimo 会为 Velox 配置 80% 的服务端内存，其余 20% 留给非 Velox 组件，例如程序二进制、HTTP streaming shuffle 和远程存储客户端等。

不过，在面对非 Velox 组件的尖峰内存使用时，Velox 自身的内存容量执行不足以防止服务端内存耗尽。例如，我们在 Prestissimo 中发现，在大型 Prestissimo 部署（>400 workers）中，HTTP streaming shuffle 可能造成很高的尖峰内存使用，很容易导致 Prestissimo worker OOM。在大型集群中，每个 worker（*PrestoExchangeSource*）可能同时从大量 source 接收 streaming data。接近 OOM 时采集到的内存 profile 显示，超过 50% 的非 Velox 内存由 HTTP proxygen 分配。为了防止 HTTP streaming shuffle 导致服务端 OOM，我们在 Prestissimo streaming shuffle 中增加了 throttle control，限制一次读取的 source 数量，从而控制 streaming shuffle 内存使用量。

除了为每个非 Velox 组件构建特定的 throttle 机制之外，我们还在 Meta Prestissimo 中提供了一个通用的服务端内存反压机制，与 Velox 协作处理非 Velox 组件的尖峰内存使用。一个 *PeriodicMemoryChecker* 在后台运行，定期检查系统内存使用量。每当系统内存使用量超过某个阈值时，它会尝试通过缩小文件缓存（*AsyncDataCache::shrink*）从 Velox 释放内存，并将释放出的缓存内存返还给 OS。通过这种方式，我们可以根据查询系统中非 Velox 组件的瞬时尖峰内存使用自动缩小文件缓存。
