数据仓库

原始数据表为 工单表 与 通话表

工单表建数据仓库可以用于业务趋势查询 通话表更多的是 设备接通

原始数据

事实  维度

1. 根据原始数据创建具有度量数据Measure与Dimension维度数据的纯业务数据表
2. 对原表的字段 区分 度量列  与 维度列；可以通过计算导出新的列；
3. 维度可以分组，如 时间维度，地域维度，业务类型，呼出线路，接通数据，通话内容；
4. 根据事实区创建Cube
5. 根据维度创建Cuboid   -- 维度集合-->表名-->分配ID
6. 根据业务数据填充Cuboid  业务数据的几种聚合类型  数量 sum min max count average
7. 根据查询条件查询输出数据；

cube名称    查询维度集合   查询的聚合表   根据查询条件得出  查询范围进行数据过滤

cube  从原始数据到cube的映射

cube : ordered dim----- index

idx + dim combine value  --> row --> json value(count sum min max)

row-filter; index;

Fact：
    row:        fact name + "|" + Now Time(8字节，long) + serial int (4字节);
    content:    Record Json Content
Cube：
    row:        cube name
    content:    id, dimensions, facts, groups(dims, vailds)
Cuboid：
    row:        cube name : A:B:C  (sorted dims)
    content:    id, enable
Voxel：
    row:        cube id + ":" + segment id + ":" + cuboid id + ":" + JSONArray(dims values).toJSONString()
    content:    json stats  {count:100, facts:{fact1:{sum, min, max},fact2:{sum, min, max}}}
Segment：
    row:        cube name + "|" + begin time + seg id
    content:    id, enable, stopTIme


服务：
1. 原始数据接收，保存到fact表；
2. 定时cube创建，生成Segment以及相应的区块数据；
3. Segment区块合并，两种方式：
    1) 合并进主区块     1 + 2 = 1; remove 2
    2) 合并进新区快     1 + 2 = 3; remove 1, remove 2
4. 查询服务
    1）如果是合并进主区，直接查询主区块
    2）如果是合并进新区，查询所有的可用区，合并查询结果；
5. cube创建服务，配置cube参数；cube初始化（cuboid 创建）；

CUBE深度优先创建；

to do:

1. Cuboids pre load;
2. Detect the scan stop time;
3. Cuboids pre build;
4. Segment combine service;
5. Query service;
6. cube Create and Config service;

配置Cube可以支持两种方式构建：
1. 渐进添加式构建； 特点：占空间少，反应快，流式，数据量小，查询更快，适用于快速的维度较少的实时数据统计
2. 分块组合式构建； 特点：占空间大，构建周期长，查询需要分块组合，好处是比较稳定

流式建仓
渐进式构建与增量式构建
数据聚合扫描
Calcite查询实现
数据片清理合并



相比于kylin,至少是10倍简化了流程，提高了效率。

1. 数据存储不需要构建维度表，不需要计算维度下标，数据以Json格式存储，可应对大规模数据集和大规模流式数据集；
2. 从一开始数据就构建在Hbase上，无需Hadoop, Hive, MapReduce, HFile, HTable加载等环节；
3. 聚合扫描大幅度简化查询和segment合并；

其他特点：

4. 提供SQL查询方式；
5. 提供的查询结果更为精确；
6. LZ4 的数据压缩大幅压缩json类型数据；
7. 近实时的分析引擎；

8. 针对Segment Time 的过滤？
9. 维度相等的过滤条件下沉到Hbase;


Voxel 改用全字符串行键，方便利用Hbase的行过滤器RegexStringComparator,SubstringComparator的过滤条件减少
Hbase到查询端的数据传输。目前只支持维度参数Equal条件下沉到Hbase.



count sum min max average distinct
一个cube最多允许一个distinct列



1. 添加HyperLogLog distinct count 实现 HLLDistinct;
2. Cube 定义包含 Dimension 列与 Measures 列，measure列为一个以":"分隔的字符串，其中字段默认为数值字段，默认提供
   min, max, sum, count, average统计; 如果measure字段形式为a.b，则不按默认处理，这种形式目前仅支持a.distinct，
   否则忽略。一个Cube最多允许一个 distinct字段；这个字段最大长度为16K长度，误差在1%左右；

集群实现：

1. 数据接收服务；
2. 任务创建服务；
3. Segment构建任务；
4. Segment合并任务；
5. Segment清理任务；
6. 查询服务；

任务领取：获取任务，如果任务已有所有人则跳过；设置任务所有人为自己. 停1秒，再次从服务器获取该任务记录，如果任务所有人还是自己，那么任务领取成功。
任务心跳：每隔10秒刷新任务心跳字段alive. 其他客户端读取该任务，发现该任务未完成且离上次刷新时间已经过去了5分钟，则可认为该任务已经失败；需要重新执行。
任务执行：因为任务的执行时间可能很长，因此当任务取得一定的进展后，更新任务进度，任务执行完设置任务状态为已完成。更新相关的Segment状态。

通过界面查看任务列表，任务进度 已完成cuboid数与当前cuboid数，算出百分比。

前台管理界面的编写。Vuetify.

取一个响亮的名字，再集成一个OLAP前端界面框架？


如何减少数据大小？cuboid按需构建（白名单钟，或者被请求过）

预购建 与 按需构建 （建立层级，父子关系）。优化父子关系。
cuboid虚转实  此时必须停止所有的 Segment构建 与 Segment合并操作。
需要实现Cuboid为虚情况下通过父Cuboid获取数据。

是否要使用维度表？是否要固定长度的数据牌子维度变量组合的前方？是否要每个Segment一个单独的HTable？


从两方面来进行优化：
1. 资源的占用；  减少不必要的Cuboid, Segment合并，通过维度表等减少单个记录的大小；
2. 查询的速度；  Voxel 行键的构成，是否缩小行键的扫描范围；过滤条件下沉，减少数据的传输；

通过反向索引减少数据量？如果能缩短Cuboding行键的长度，反向索引是一种方式。

年  3
季  3
月  2
周  3
天  7
18个Segment

流式实时构建，最后一天的数据渐进式添加，这样可以减少碎片化segment数量。如果渐进式构建出错，可以重建最后一天的数据。

Cuboid按需添加产生的Voxels必须存放在相应的Segment中，否则查询时没法按Segment Time进行过滤

是否考虑所有的Cube的Segment ID 按同一个序列增长？ 可以节省一些Voxel键的长度

Primary Segment   Active Segment

构建服务在同一个方法中，按次序执行，不同的Cube可以并行执行：

1. Cuboid 新增填实；
    1）扫描所有新增，获取最上层的一个；
    2）寻找最合适的父节点，按区块 填实Voxels；
    3）优化父子结构；
    4）返回1）执行；
    5）Cuboid填实可以按Segment并行进行？ 可以
2. Active Segement 新增：每5分钟把新的数据合并进Active Segment;
    0）检查是否有未完成的新增任务，如果有，则清理掉；
    1）检查是否存在Active Segment, 如果不存在，创建，以 00:00:00 为时间分割；
    2）检查Active Segment是否满足完整一天的时间范围，满足更改类型为Day, 创建新的Active Segment;
    3) 创建新的任务，把新产生的数据合并进Active Segment; 成功后删除任务，否则任务留下来，代表任务失败了；
3. Segment合并：
    1）7个完整的天 合并一周；
    2）4个完整的周 合并一月；
    3）3个完整的月 合并一季；
    4）4个完整的季 合并一年；
    5）5个完整的年 合并一个Primary;

查询时可以设置一个 SegmentTime>时间 的过滤条件，用于减少需要扫描的Segment数；

流式构建：
全量构建：

大批量构建：直接构建成一个，最大单Segment构建。衡量方法，单次原始Cobodings不超过50万？



如何处理全量构建？
先按单次最大量/批量构建，如果构建数多余1个Segment，最后来一次整体合并。

如何处理增量构建？
先按单次最大量/批量构建，如果构建数多余1个Segment，最后来一次整体合并为增量Segment。

根据原始构建进行切割？


对原始fact进行处理，生成原始Voxels,按Segment分片存储，后续处理，按

思考：

如何协调3种任务之间的关系？

增量构建 与 区块合并 是横向任务
Cuboid填充 是纵向任务

横向任务 和 纵向任务不能同时进行。

用Increament获取任务的唯一所有权。流程如下：
假设任务的原始状态为0，如果获取增量操作结果为1，那么得到所有权，否则没得到。
任务结束后设置该状态为0，释放任务。

如此，可以进行多任务协调处理，集群运行。
需要有一个



1. Obase Entity对一些需要考虑并发存储的数据采用B(Directly) Family. 支持checkAndPut操作，支持Inreament操作。可用于资源抢占实现。
2. Active Segment完美的切割方案；
    1) 按天00:00:00点切分最好，同时需要考虑效率；
    2) 最小时间范围为一天；一个日期？
    3) 如果有历史数据，允许胖天？
3. Cuboid 条数科学的的计数方案；
    1) 需要多个Segment综合；
    2) 需要考虑Segment移除时计数更新；
    3) 需要与后期新增Cuboid的计数方案一致；
    4) 可以考虑在Cuboid中增加Segment分片计数，可以支持Segment移除操作，总数计数时统合；
    5) Cuboid增加查询计数，采用Increament字段？
4. 查询如果Cuboid没有预构建，如何处理？
    1) 从父级获取数据，目前看来无法做好合并处理；
    2) 查询时返回错误，提交构建任务；
5. 同时支持一个字段既为度量，也为维度；



ODBC查询的支持


需要优化区块范围分割   计时采用不重复的nano计时   fact->segment


优化了处理速度，可以采用从其他数据库导入数据分析的方式，扩展应用面，作为一个专用的BI工具。
查询可以设定最多返回数据不超过50000条， 在查询sql中自动添加limit条件？



如何处理千亿级别、PB级别的数仓构建？
1. fact 表写 Endpoint Coprocessor 协处理器;
2. 调用协处理器并发生成分片 Segments;
3. 用聚合扫描生成统一数据;

2，3


项目，Cube
Project.Cube

考虑到不同的项目中可能有相同名字的Cube, 因此 Cube的全名应该更改为：ProjectName + "." + CubeName


OLAP引擎部署：
1. Fact 入库服务；
2. Cube 构建、Segment和并、Cuboid添加;
3. Rest Query服务；

CDH部署

中间件
RabbitMQ部署
Apache-Superset部署
Cube操作前端部署

如何处理日增千亿级别数据的处理？

1. fact分区入库；
2. cube指定路由；单机多fact并行；
3. fact推式聚合扫描；

表分区  fact分区基数  voxel分区基数

fact

fact添加partition
cube参数分partition,用于查询时合并。
cube新建服务需要一个服务器给cube building划分分区任务


新增Job管理服务

集群实现

Cuboding多分区实现-->任务分发，管理机制实现-->kafka流数据处理实现

0.5 manager + builder 分布式构建服务实现



数据仓库

原始数据表为 工单表 与 通话表

工单表建数据仓库可以用于业务趋势查询 通话表更多的是 设备接通

原始数据

事实  维度

1. 根据原始数据创建具有度量数据Measure与Dimension维度数据的纯业务数据表
2. 对原表的字段 区分 度量列  与 维度列；可以通过计算导出新的列；
3. 维度可以分组，如 时间维度，地域维度，业务类型，呼出线路，接通数据，通话内容；
4. 根据事实区创建Cube
5. 根据维度创建Cuboid   -- 维度集合-->表名-->分配ID
6. 根据业务数据填充Cuboid  业务数据的几种聚合类型  数量 sum min max count average
7. 根据查询条件查询输出数据；

cube名称    查询维度集合   查询的聚合表   根据查询条件得出  查询范围进行数据过滤

cube  从原始数据到cube的映射

cube : ordered dim----- index

idx + dim combine value  --> row --> json value(count sum min max)

row-filter; index;

Fact：
    row:        fact name + "|" + Now Time(8字节，long) + serial int (4字节);
    content:    Record Json Content
Cube：
    row:        cube name
    content:    id, dimensions, facts, groups(dims, vailds)
Cuboid：
    row:        cube name : A:B:C  (sorted dims)
    content:    id, enable
Voxel：
    row:        cube id + ":" + segment id + ":" + cuboid id + ":" + JSONArray(dims values).toJSONString()
    content:    json stats  {count:100, facts:{fact1:{sum, min, max},fact2:{sum, min, max}}}
Segment：
    row:        cube name + "|" + begin time + seg id
    content:    id, enable, stopTIme


服务：
1. 原始数据接收，保存到fact表；
2. 定时cube创建，生成Segment以及相应的区块数据；
3. Segment区块合并，两种方式：
    1) 合并进主区块     1 + 2 = 1; remove 2
    2) 合并进新区快     1 + 2 = 3; remove 1, remove 2
4. 查询服务
    1）如果是合并进主区，直接查询主区块
    2）如果是合并进新区，查询所有的可用区，合并查询结果；
5. cube创建服务，配置cube参数；cube初始化（cuboid 创建）；

CUBE深度优先创建；

to do:

1. Cuboids pre load;
2. Detect the scan stop time;
3. Cuboids pre build;
4. Segment combine service;
5. Query service;
6. cube Create and Config service;

配置Cube可以支持两种方式构建：
1. 渐进添加式构建； 特点：占空间少，反应快，流式，数据量小，查询更快，适用于快速的维度较少的实时数据统计
2. 分块组合式构建； 特点：占空间大，构建周期长，查询需要分块组合，好处是比较稳定

流式建仓
渐进式构建与增量式构建
数据聚合扫描
Calcite查询实现
数据片清理合并



相比于kylin,至少是10倍简化了流程，提高了效率。

1. 数据存储不需要构建维度表，不需要计算维度下标，数据以Json格式存储，可应对大规模数据集和大规模流式数据集；
2. 从一开始数据就构建在Hbase上，无需Hadoop, Hive, MapReduce, HFile, HTable加载等环节；
3. 聚合扫描大幅度简化查询和segment合并；

其他特点：

4. 提供SQL查询方式；
5. 提供的查询结果更为精确；
6. LZ4 的数据压缩大幅压缩json类型数据；
7. 近实时的分析引擎；

8. 针对Segment Time 的过滤？
9. 维度相等的过滤条件下沉到Hbase;


Voxel 改用全字符串行键，方便利用Hbase的行过滤器RegexStringComparator,SubstringComparator的过滤条件减少
Hbase到查询端的数据传输。目前只支持维度参数Equal条件下沉到Hbase.



count sum min max average distinct
一个cube最多允许一个distinct列



1. 添加HyperLogLog distinct count 实现 HLLDistinct;
2. Cube 定义包含 Dimension 列与 Measures 列，measure列为一个以":"分隔的字符串，其中字段默认为数值字段，默认提供
   min, max, sum, count, average统计; 如果measure字段形式为a.b，则不按默认处理，这种形式目前仅支持a.distinct，
   否则忽略。一个Cube最多允许一个 distinct字段；这个字段最大长度为16K长度，误差在1%左右；

集群实现：

1. 数据接收服务；
2. 任务创建服务；
3. Segment构建任务；
4. Segment合并任务；
5. Segment清理任务；
6. 查询服务；

任务领取：获取任务，如果任务已有所有人则跳过；设置任务所有人为自己. 停1秒，再次从服务器获取该任务记录，如果任务所有人还是自己，那么任务领取成功。
任务心跳：每隔10秒刷新任务心跳字段alive. 其他客户端读取该任务，发现该任务未完成且离上次刷新时间已经过去了5分钟，则可认为该任务已经失败；需要重新执行。
任务执行：因为任务的执行时间可能很长，因此当任务取得一定的进展后，更新任务进度，任务执行完设置任务状态为已完成。更新相关的Segment状态。

通过界面查看任务列表，任务进度 已完成cuboid数与当前cuboid数，算出百分比。

前台管理界面的编写。Vuetify.

取一个响亮的名字，再集成一个OLAP前端界面框架？


如何减少数据大小？cuboid按需构建（白名单钟，或者被请求过）

预购建 与 按需构建 （建立层级，父子关系）。优化父子关系。
cuboid虚转实  此时必须停止所有的 Segment构建 与 Segment合并操作。
需要实现Cuboid为虚情况下通过父Cuboid获取数据。

是否要使用维度表？是否要固定长度的数据牌子维度变量组合的前方？是否要每个Segment一个单独的HTable？


从两方面来进行优化：
1. 资源的占用；  减少不必要的Cuboid, Segment合并，通过维度表等减少单个记录的大小；
2. 查询的速度；  Voxel 行键的构成，是否缩小行键的扫描范围；过滤条件下沉，减少数据的传输；

通过反向索引减少数据量？如果能缩短Cuboding行键的长度，反向索引是一种方式。

年  3
季  3
月  2
周  3
天  7
18个Segment

流式实时构建，最后一天的数据渐进式添加，这样可以减少碎片化segment数量。如果渐进式构建出错，可以重建最后一天的数据。

Cuboid按需添加产生的Voxels必须存放在相应的Segment中，否则查询时没法按Segment Time进行过滤

是否考虑所有的Cube的Segment ID 按同一个序列增长？ 可以节省一些Voxel键的长度

Primary Segment   Active Segment

构建服务在同一个方法中，按次序执行，不同的Cube可以并行执行：

1. Cuboid 新增填实；
    1）扫描所有新增，获取最上层的一个；
    2）寻找最合适的父节点，按区块 填实Voxels；
    3）优化父子结构；
    4）返回1）执行；
    5）Cuboid填实可以按Segment并行进行？ 可以
2. Active Segement 新增：每5分钟把新的数据合并进Active Segment;
    0）检查是否有未完成的新增任务，如果有，则清理掉；
    1）检查是否存在Active Segment, 如果不存在，创建，以 00:00:00 为时间分割；
    2）检查Active Segment是否满足完整一天的时间范围，满足更改类型为Day, 创建新的Active Segment;
    3) 创建新的任务，把新产生的数据合并进Active Segment; 成功后删除任务，否则任务留下来，代表任务失败了；
3. Segment合并：
    1）7个完整的天 合并一周；
    2）4个完整的周 合并一月；
    3）3个完整的月 合并一季；
    4）4个完整的季 合并一年；
    5）5个完整的年 合并一个Primary;

查询时可以设置一个 SegmentTime>时间 的过滤条件，用于减少需要扫描的Segment数；

流式构建：
全量构建：

大批量构建：直接构建成一个，最大单Segment构建。衡量方法，单次原始Cobodings不超过50万？



如何处理全量构建？
先按单次最大量/批量构建，如果构建数多余1个Segment，最后来一次整体合并。

如何处理增量构建？
先按单次最大量/批量构建，如果构建数多余1个Segment，最后来一次整体合并为增量Segment。

根据原始构建进行切割？


对原始fact进行处理，生成原始Voxels,按Segment分片存储，后续处理，按

思考：

如何协调3种任务之间的关系？

增量构建 与 区块合并 是横向任务
Cuboid填充 是纵向任务

横向任务 和 纵向任务不能同时进行。

用Increament获取任务的唯一所有权。流程如下：
假设任务的原始状态为0，如果获取增量操作结果为1，那么得到所有权，否则没得到。
任务结束后设置该状态为0，释放任务。

如此，可以进行多任务协调处理，集群运行。
需要有一个



1. Obase Entity对一些需要考虑并发存储的数据采用B(Directly) Family. 支持checkAndPut操作，支持Inreament操作。可用于资源抢占实现。
2. Active Segment完美的切割方案；
    1) 按天00:00:00点切分最好，同时需要考虑效率；
    2) 最小时间范围为一天；一个日期？
    3) 如果有历史数据，允许胖天？
3. Cuboid 条数科学的的计数方案；
    1) 需要多个Segment综合；
    2) 需要考虑Segment移除时计数更新；
    3) 需要与后期新增Cuboid的计数方案一致；
    4) 可以考虑在Cuboid中增加Segment分片计数，可以支持Segment移除操作，总数计数时统合；
    5) Cuboid增加查询计数，采用Increament字段？
4. 查询如果Cuboid没有预构建，如何处理？
    1) 从父级获取数据，目前看来无法做好合并处理；
    2) 查询时返回错误，提交构建任务；
5. 同时支持一个字段既为度量，也为维度；



ODBC查询的支持


需要优化区块范围分割   计时采用不重复的nano计时   fact->segment


优化了处理速度，可以采用从其他数据库导入数据分析的方式，扩展应用面，作为一个专用的BI工具。
查询可以设定最多返回数据不超过50000条， 在查询sql中自动添加limit条件？



如何处理千亿级别、PB级别的数仓构建？
1. fact 表写 Endpoint Coprocessor 协处理器;
2. 调用协处理器并发生成分片 Segments;
3. 用聚合扫描生成统一数据;

2，3


项目，Cube
Project.Cube

考虑到不同的项目中可能有相同名字的Cube, 因此 Cube的全名应该更改为：ProjectName + "." + CubeName


OLAP引擎部署：
1. Fact 入库服务；
2. Cube 构建、Segment和并、Cuboid添加;
3. Rest Query服务；

CDH部署

中间件
RabbitMQ部署
Apache-Superset部署
Cube操作前端部署

如何处理日增千亿级别数据的处理？

1. fact分区入库；
2. cube指定路由；单机多fact并行；
3. fact推式聚合扫描；

表分区  fact分区基数  voxel分区基数

fact

fact添加partition
cube参数分partition,用于查询时合并。
cube新建服务需要一个服务器给cube building划分分区任务


新增Job管理服务

集群实现

Cuboding多分区实现-->任务分发，管理机制实现-->kafka流数据处理实现

0.5 manager + builder 分布式构建服务实现


优化Cuboid添加门槛


基于Region删除的数据清理。
Segment生成是需要有序还是无序？ 无序均衡写入，有序便于清理。

优化输入 -- kafka
优化逻辑 -- 数据表示优化，无维基关联表，优化维度组合
优化处理 -- 集群并行处理，优化算法增强单节点处理能力
优化读写 -- 水平分区，聚合扫描
优化过滤 -- segment存储维度最大最小值
优化清理 -- 基于region的segment清理

1. 多节点集群处理；
2. 写入 --> 合并
3. 区块维度最大最小值；
4. 优化度量表示；增加聚合速度；
5. 优化维度降维聚合速度；
6. 优化Cuboid，去除无效、低效组合；
7. 实现基于region的数据清除；
8. 基于segment的更为均衡的实现；
9. 查询时的有效维度组合统计


基于时间的过滤
保留时间字段：year month day hour minute second, 自动排列在维度的最前面 都是String类型   ok


5分钟 --> 20分钟 --> 1小时 --> 6小时 --> 24小时
4         3          6          4

最小的5片合并


voxel行键升级为：
partid(byte) + cube id(int) + segment id(int) + cuboid id (int) + 维度聚合值数组json化字符串
前面13个字节为固定信息

5分   25分   2小时  10小时    2天   10天    50天   250天


在partion水平分区的基础上，在添加垂直分区，原理可参考java垃圾收集中的年轻代，年老代概念；
垂直分区分为天、周、月、季、年，具体实现采用segment编号聚集实现；
为实现编号聚集，所有的cube采用一个全局的segment编号分配器
天、周、月、季、年的segment段分别为 0 --> 1亿 --> 2亿 --> 3亿 --> 4亿 --> 5亿
segment编号单调增长，达到最大值后回归起点，重新循环；
天区中包含区段类型包括：
及时生成（1分钟）--> 5分钟 --> 30分钟 --> 4小时 --> 24小时 天
 0 --> 1亿 --> 2亿 --> 3亿 --> 4亿 --> 5亿 --> 6亿 --> 7亿 --> 8亿 --> 9亿
 1分   5分     30分    4小时   1天     1周      1月    1季     1年     5年
 5      6      8       6       7       4       3       4      5
随着时间推移，各区块逐渐合并升级，前面的区块逐渐过期，相应的region也会逐渐过期，利用region
数据删除机制批量删除过期数据；
每个partion预先构建天、周、月、季、年分区，以 partid + segment分区起点 为startKey

删除region删除过程：
1. 检查region中的数据是否全部过期；
2. region是否处于split状态，是否daughter;
3. region已处于无引用状态, flush region; delete region files;
4. 通过 admin.split() admin.merge() 添加region和删除region;
5. 定时任务通过region清除定期清理失效segment数据；
6. region数据删除后，空区与后续数据合并？还是多空区合并为单空区？

voxel的rowkey需要重新调整parid + segment id + cuboid id .....
cube的数据扫描只能是基于区块分段实现



通过RegionObserver删除Region:
1. HRegion.close();
2. Delete


简单实现：
reverse(segment id),


不通的cube，因为生产数据的速度不一样，他们每个阶段的生命周期会不一样
流水化生产线


version 0.2
预计将实现：
1）本版本Voxel将采用 水平 + 时间 预分区策略，预分区数为： 8 * 10 = 80个
Voxel数据删除将采用直接删除方式，定时检查region数据是否失效，如果某个region的所有数据
都已处于删除状态主动触发region级别major_compact清除数据，降低hbase空间占用;
2）Segment id将采用按时间分区全局分配，调整Voxel行键结构；


通过RegionObserver删除Segment数据，降低网络负载，降低处理机压力。

分区，方便控制region数量，读写相对比较集中，清理效率高，服务器压力较小
随机，读写均衡，清理粘边可能性比较大；
随机+region隔离，清除方便，写入均衡，问题是导致region过多

region过多可以缩减partition; 减少小的segment；
partion: 8-->2  根据数据量

segment生成完毕，调用flush刷写到磁盘，降低memstore占用
cube可以分为流式和渐增，增量式可以减少微小的segment生成。 或者cube可以设置读取周期。超过读取周期，或者数据达到单次处理最小阀值，
启用segment构建；
每次segment构建完毕。或者cuboid添加完毕，调用flush刷写到hdfs.



1. 预分区只有partion;
2. segment id采用 discret long id;
3. 每个segment独占region,采用前切割
4. 去除clean job;
5. 直接删除segment ---region;
6. flush region 减少占用memstore占用；
7. 无随机读写；
8. 1000个cube, 200000region ?

基于有些预切分可能失败，则调整为概率预切分，容忍失败；
删除也调整为概率删除；
怎么断定一个一个region已进入老年期？


策略一：

优点：读写均衡，清理及时  缺点：region 多

1. 创建segment时执行预分区；
2. (服务，每5分钟执行) 执行region扫描，segment扫描，清理所有segment数据已经过期的region;
3. (服务，每5分钟执行) 执行region扫描，合并内容为空的region;
4. (服务，每30分钟执行) 在凌晨2点-6点，对segment粘在一起的region进行split;

策略二：

优点：region 少  缺点：清理不及时，读写均衡性较差

1. 顺序 segment 按时间粒度大小 segment分段；
2. (服务，每5分钟执行) 执行region扫描，segment扫描，清理所有segment数据已经过期的region;
3. (服务，每5分钟执行) 执行region扫描，合并内容为空的region;



清理region reference.

切分时，通过切分的 splitKey 可以查找到父region;
切分后，空切分，保留父region,生成一个空的daughter


byte[] part   0-7
byte[] region 0-31 8*128
8 * 256     40*1000


256region

第一个字节 partion  第2字节 segment hash

observer

1. segment combine  -- HFile -- Bulkload
2. 禁用 major compact;
3. mini compact 通过TTL删除 store file;
4. mini compact 通过InternalScanner 删除失效的数据；
5. 通过 segment --> partid + segment hash 定位region;
6. 基于 segment 的操作；
7. 主动调用仅需支持 combine 命令，以及进度查询；combine 完成enable target segment，disable source segment;
8. 失效segment的主动获取； -- 当前最大segment id,

一期：
先前可仅实现 InternalScanner 删除失效数据过滤；
如果发现StoreFile中有失效文件，优先删除失效文件；

二期：
通过region observer执行segment combine;

version 0.4:

更改Observer的实现方式, segment region处理仅保留flush segment功能


segment id 顺序产生，voxel

CombineSegment_SubTask 分区
CombineSegment_Task

AddCuboid_Task




builder-api
core  核心功能
observer  协处理器
query 查询服务
master 主控
jdbc

初始构建:  builder-api
combine:  observer
+cuboid:  segment补全


中途停止？


observer存在bug
+cuboid 存在bug




+cuboid:
1. 候选  待选状态
2. 补全  新建时需要，合并时检查（需要等待所有合并源补全），正式转入此状态时添加补全任务（）；
3. 正式  新建时需要，查询可以直接使用
候选  填充  正式   废弃
candidate charging productive deprecated

+segment:  类型分为 原生 与 组合
1. 原生   创建 --> 填实 --> 完成
2. 组合   创建 --> 填实(生成子任务， 等待完成) --> 完成(先启用结果，再disable源，查询时做排他性检测)
新建  填充  正式   合并  归档  删除
created charging productive merging archive

所有的操作都是以segment为原子


处于合并状态的segment不能填充cuboid

补全任务 + 合并任务  在单个定时任务中生成，此两种状态不能同时作用

任务状态： 创建  执行 完成/错误 归档  删除
created execute complete failed archive

合并任务优先 ---> 其次 +cuboid

合并完成的segment需要检查是否需要+cuboid

合并完成（合并任务所相应的分区合并任务都已经完成）
补全完成（立方体所有有效分片的补全任务都已完成）

Cuboid检查
分片检查
分片执行
Cuboid执行


segment清理
segment build 检查
segment combine 检查
segment combine 任务生成
segment combine 提交执行

cuboid add 任务检查
cuboid add 任务生成
cuboid add 提交执行


流程：
segment build 检查
segment combine 检查
cuboid add 任务检查
segment combine 任务生成
cuboid add 任务生成
segment combine 提交执行
cuboid add 提交执行


优化Cuboid生成 最大限度减少低效维度组合
1. 对于维度分组中的维度，生成两两组合Cuboid;
2. 生成单个维度的Cuboid;
3. 寻找维度的附属维度  单维度RecordCount*1.1 < 多维度RecordCount
4. 生成查询组合时  添加所有附加维度;


数据权限的处理：
用户表  用户名  密码
project - cube


用String字段实现标签集合操作 [标签] 用于不定维度分析 此类型命名为？ tagset   对应于sql的varchar java的string

CuboidAdd究极优化方案：
各Region，各Part，本地读取，并行执行。
使用Hbase数据锁

随时添加查询组合优化是最大的特点，能够实时做出好的优化结果，让用户减少等待时间是最重要的。

1个CuboidAdd，多个任务。增加一个同步锁。获取锁以后可以更新，用完以后释放。part操作完后添加完成集合中添加标志。


最终采用并行优化方式优化CuboidAdd执行

在摧毁Cube的时候，相关的Task没有删除，会导致错误。。。


计数统计
bitcount   hllcount


多种类型的数据源
1. 自己通过Olap.prepareSegment提交；
2. 通过builder服务提供的rest接口提交；
3. 其他开发的数据库定时监控接口，比如基于自增id ....


fact表采用分区设计，均衡fact表读写负载

feature list:
1. 没有基维，所有数据都以JSON格式存储；
2. 没有成千上万的hbase表，只有几个干干净净的表；
3. 除Hbase外，无需复杂的架构支持（单机hbase也可以，hbase集群提供更贵性能）；
4. 处理能力可以水平扩展，从每秒数千到每秒千万级数据都可以实时处理；
5. 支持原始数据删除，只需要原来推的数据加一个删除标签重推一次；

deep optimize:
6. 聚合数据直接以文件方式写入和删除，最大限度的扩展hbase吞吐能力；
7. 垂直、水平预分区，并行读写均衡hbase访问压力；

8.



数据更新    通过数据删除和添加数据实现更新
数据轨迹    针对某个对象的所有非删除数据的集合按时间排序就是数据轨迹
标签新增    是预先空出待使用标签维度？count/min/max/sum  count/sum count
标签查询    id列？
数据缓存    实时表


FACT数据唯一性： 纳秒级时间戳 + 随机数或者UUID

维度组合：
原始数据 骨干数据 扩展数据

从时间维度筛选 从业务体维度赛选 。。。 维度组合的维度排序问题



基于id的垂直查询 查询id相关的所有历史数据 id数据最适合于全维度数据.
但是对于一个可更新数据来说，同一id的数据会多次出现，那么基于同一id的聚合的需求也很重要。
