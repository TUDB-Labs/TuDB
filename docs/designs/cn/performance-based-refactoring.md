# 针对性能调优的查询引擎执行层重构

## 问题

目前TuDB的查询引擎Lynx虽然实现了基本功能，但是代码结构不够清晰，实现不够高效，执行层问题尤为明显，甚至会阻碍性能分析。我们希望通过对其重构，让性能分析变得可能，优化一些显而易见的低效实现，同时提高其代码质量，使其更容易让人理解和修改。

## 分析

目前Lynx把一个查询转换为物理计划后，表面上执行发生在各个物理节点对PPTNode::execute()的具体实现，其结果为一个DataFrame对象；实际的执行方式记录在DataFrame对象的records Iterator里，以匿名函数的形式存在（参见DefaultDataFrameOperator），并在用户真正使用这个Iterator提取结果时触发执行（目前在QueryService生成json结果时调用）。表面上我们在“执行”物理计划，实际上是生成了一个执行计划树。树的节点是各个运算符如filter/project/join，执行时节点之间传递的是带有特定格式的单行数据，具体表示为Seq[LynxValue]。
这样有如下几个问题：
* 运算符按行传递数据，近似于解释执行，CPU开销很高（并非当前瓶颈，解决方案不在此设计范围内）；而且导致无法记录每个算子用了多长时间，每行记录一次不准确而且开销大。
* 查询执行阶段的性能分析多依赖于Profiling：profiling工具收集各个时间点的stacktrace，而后通过stack frame中出现的类/函数名来估算它们占用的时间，结果多为火焰图。运算符均为匿名函数时，profiling无法有效指出每个运算符使用的时间。
* 运算符没有任何的单元测试，也由于全都是DefaultDataFrameOperator里的匿名函数，也不好单元测试。
* 所有的运算符均放在DefaultDataFrameOperator里，导致其过于臃肿。运算符调用并非显式函数调用，而是使用隐式类型转换(Scala implicit def)直接在DataFrame对象上调用，不够直观也不易寻找。

## 方案

我们可以把执行计划树做成一个显式的结构。各个运算符划分成单独的类（同时添加单元测试），并统一继承一个类似Iterator的父类：
```
 trait ExecutionOperator extends TreeNode {
   // Input operators
   val children: Seq[ExecutionOperator]

   // Prepare for processing.
   def Open() = {
       ...common logic...
       OpenImpl()
   }
   
   // To be implemented by concrete operators.
   def OpenSelf();
   
   // Fills output_batch. Empty RowBatch means the end of output.
   def GetNext(output_batch: RowBatch)  = {
     ...common logic...
     // Example way of collecting per-operator metrics.
     ScopedTimer timer;
     return GetNextImpl(output_batch);
   }
   
   // Interface for child class to override
   def GetNextImpl(output_batch: RowBatch); 
   
   // Ends processing.
   def Close() = {
       ...common logic...
       CloseImpl()
   }
   
   // To be implemented by concrete operators.
   def CloseImpl();
   
   // Schema of output rows.
   def OutputSchema()
 }
 
 // Example concrete operator.
 class FilterOperator extends ExecutionOperator {
   override def OpenImpl() = {
     children[0].Open()
   }
   
   override def GetNextImpl(output_batch: RowBatch): Unit = {
     while (!output_batch.full()) {
       RowBatch input_batch;
       children[0].GetNext(input_batch)
       if (input_batch.empty()) break;
       RunFilter(input_batch, output_batch)
     }
   } 
   
   override def CloseImpl() = {
     children[0].Close()
   }
 }
```

运算符的整个周期是Open->GetNext->Close，其中GetNext可以反复调用直到不能再返回更多结果。父类ExecutionOperator中对这三个接口有统一的基本实现，用来做一些所有操作符都通用的事情，比如收集metrics，各操作符独自的逻辑在对于的Impl方法里实现。在执行计划生成后，只需像对Iterator一样从根节点获取输出，各节点内部会层级调用各自子结点的Open/GetNext/Close。

```
ExecutionOperator execution_plan = GenerateExecutionPlan(physical_plan)

execution_plan.Open()
while (true) {
    RowBatch output_batch;
    execution_plan.GetNext(output_batch);
    if (output_batch.empty()) break;
    // consume output here
}
execution_plan.Close()
```

运算符之间传递的单元为RowBatch，包含多个输出行。具体行数可以先用固定值比如256。将来有内存管理后需要按照实际占用内存大小决定行数。

```
class RowBatch {
  val rows: Seq[Seq[LynxValue]]
}
```

从物理计划到执行计划的转换方式可以参照逻辑计划到物理计划的转换。

##  实施

* 添加ExecutionOperator和RowBatch基本类别
* 针对每种运算，添加一个继承ExecutionOperator运算符，逻辑可以照抄相应的物理计划节点的execute方法及DataFrameOperator中的运算方法（如select/join)，然后添加单元测试。单元测试中，对单个运算符执行完整的Open/GetNext/Close周期，然后检查结果是否正确。为了方便测试，增加一个可定制输出数据FakeOperator，用做被测运算符的输入运算符。
* 所有运算符添加完毕后，添加ExecutionPlanGenerator，用于将物理计划转为执行计划。
* 将ExecutionPlanGenerator应用在CypherRunner中，取代physicalPlan.execute()。处理输出时，可以写一个Iterator将最终的RowBatch stream转为LynxResult.records要求的stream，这样可避免使用LynxResult的其他部件。
* 清理DataFrame/DataFrameOperator/DataFrameOps及所有的PPTNode子类的execute方法
