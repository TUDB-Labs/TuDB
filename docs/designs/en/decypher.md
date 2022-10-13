## De-Cypher

### Problem

Lynx relies on openCypher for parsing queries. However, Cypher constructs penetrates almost all layers of Lynx besides parsing, including logical plan, physical plan and execution plan. Take projection as example: LogicalProject has Cypher ReturnItemsDef; PhysicalProject has its too; ProjectOperator doesn’t have it, but still has Cypher Expression. The same pattern appears in most operators.

This poses several problems. 
* Cypher concepts don’t always map to Lynx concepts. For example, Cypher ReturnItemsDef represents a list of items in RETURN clause, which can be simple projection or complex aggregation. After being converted to logical/physical plan, it can be vastly different operators. 
* (As a result of item 1) Cypher constructs have information that’s unnecessary for different stages of query processing. For example, for physical Project, we only need a list of Expressions for projection, but Cypher ReturnItemsDef much more than that. As a result, when we need to build an Expression, we have to set many unnecessary parameters. For example, all Cypher expressions require a `position` parameter (because Cypher needs to report where parsing error happens), but this is totally unnecessary for query planning/execution.
* (As a result of item 1) We cannot easily change Cypher for our convenience. For example, if we want to fixate intermediate row schema to certain offsets, there is no easy way to attach offsets  Cypher Variables. Also, there is no type info in Cypher Expression and we cannot easily add it, which makes type-specific (for example, null or non-null) optimizations impossible.
Incompatible Cypher changes can break relevant parts in Lynx.
 
### Solution

We should only use openCypher for parsing. During logical planning, we should convert Cypher constructs to our own representation and keep using it for the rest of query processing.
 
The gap between current state and the desired state includes three parts:
* Logical nodes. Existing logical nodes are mostly wrappers of relevant Cypher constructs. We should extract useful info from Cypher constructs and add them as fields of logical node. For example, LogicalProject should only get the list of projection expression and output column names.
* Physical nodes. Same as logical node.
* Expressions. This requires our own Expression representation. It will be similar to Cypher Expression. But we can trim/add fields according to our need. There are two main changes we need for now: 
	* Add offset in Variable expression. This will allow us to get variable value by offset instead of name lookup during evaluation.
	* Add type info in each expression. Leaf nodes of Expression tree like Literal or Variable should have given type. Intermediate node like FunctionCall should be able to deduct the output type.

### Implementation plan

Since expression library is the cornerstone, we will start with adding our own version. Instead of adding the entire expression library and  changing all layers in one shot, we can actually do it incrementally. 

First we can add base class LynxExpress and make it extend Cypher Expression. Then we add a conversion util for converting Cypher Expressions that are supported by our expression library to our own version. In each logical translation, when we see any Cypher Expression, we call this util to rewrite the expression tree. Notice this rewrite can happen at any level of expression tree, for example, in Add(VariableA, VariableB), we might support Variable before Add. And the tree after rewrite can have both Cypher Expression and Lynx Expression.

Then we can add concrete expression one by one without breaking existing code. Each concrete Expression is a subclass of base LynxExpression. After adding each one, we should also update the expression evaluator to correctly process it and update the conversion util to do the rewrite.

Given there is dependency from execution operator to physical node and from physical node to logical node, we should implement the change from execution layer first and then physical planning and logical planning. 

Eventually, after all changes are done, we can stop making LynxExpression extend Cypher Expression and this will be the final cut between Lynx expression and Cypher expression.

