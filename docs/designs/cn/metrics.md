Metrics Expose Design

1. 设计目标
系统metrics用于监控图数据库系统运行的情况。主要包含以下方面：
特定操作需要的时间。例如，单个查询的耗时。
自定义关键参数。例如，对于不同的operator，可能需要输出不同的信息。例如读写操作时，读写的数据大小。
系统资源参数。例如，CPU和内存利用率。

我们将metrics expose分成两个阶段完成。

1.1 第一阶段
第一阶段主要收集单个query的性能信息：
单个query的耗时
这个query里面，每个operator的耗时
每个operator相关的信息。这些信息每个operator是不同的

1.2 第二阶段
这一阶段主要完成系统整体性能的监控：
所有query性能相关参数的聚合。例如95% query的耗时。
系统级别的性能和资源参数。例如吞吐和资源利用率。

现阶段我们主要专注于第一阶段的实现。

2. Metrics设计

这里我们提出一个metrics domain的设计。每个domian里面只包含这个domain相关的日志。我们可以对一个domian的日志进行聚合，分解并通过不同的view展示。但不同domian的日志不能进行聚合展示。

日志事例：

DomainID: 日志所属的domian的id。输出任何日志前，必须先创建一个日志domian。Domain id可以自定义。如果用户没有定义，系统会自动生成一个独特的id。
Labels：完全由用户自定义，一条日志可有有多个labels（用;隔开），也可以为空。
Timestamp：记录日志的时间。
Type：日志的数据格式。例如，string, int, float等。
Log: 日志。

下面我们用具体的例子来解释metrics的使用。

假设我们需要追踪一个query的性能。Domain id就是一个query的id。

如果我们需要记录这个query的latency：

如果我们需要记录某个operator的latency：

同理，如果我们需要记录某个operator的其他参数：


在进行展示的时候，我们可以对有相同label的日志数据进行聚合。例如，我们希望了解每个operator latency的占比，就可以对Query1的日志中，过滤出有OperatorLog和Latency两个label的日志进行聚合。如果我们希望查看operator1的相关信息，只需要过滤出有OperatorLog和Operator1两个label的日志。