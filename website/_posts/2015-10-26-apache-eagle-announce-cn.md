---
layout: post
title:  "Apache Eagle 正式发布：分布式实时Hadoop数据安全方案"
date:   2015-10-26 19:24:33
author: Hao Chen, Edward Zhang, Libin Sun, Jilin Jiang, Qingwen Zhao
categories: post
---

> *摘要*：日前，eBay公司隆重宣布正式向开源业界推出实时分布式Hadoop数据安全方案 - Apache Eagle，作为一套旨在提供高效分布式的流式策略引擎，并集成机器学习对用户行为建立Profile以实时智能地保护Hadoop生态系统中大数据安全的解决方案。

日前，eBay公司隆重宣布正式向开源业界推出分布式实时安全监控方案 － Apache Eagle (http://goeagle.io)，该项目已于2015年10月26日正式加入Apache 成为孵化器项目。Apache Eagle提供一套高效分布式的流式策略引擎，具有高实时、可伸缩、易扩展、交互友好等特点，同时集成机器学习对用户行为建立Profile以实现智能实时地保护Hadoop生态系统中大数据的安全。

## 背景
随着大数据的发展，越来越多的成功企业或者组织开始采取数据驱动商业的运作模式。在eBay，我们拥有数万名工程师、分析师和数据科学家，他们每天访问分析数PB级的数据，以为我们的用户带来无与伦比的体验。在全球业务中，我们也广泛地利用海量大数据来连接我们数以亿计的用户。

近年来，Hadoop已经逐渐成为大数据分析领域最受欢迎的解决方案，eBay也一直在使用Hadoop技术从数据中挖掘价值，例如，我们通过大数据提高用户的搜索体验，识别和优化精准广告投放，充实我们的产品目录，以及通过点击流分析以理解用户如何使用我们的在线市场平台等。

目前，eBay的Hadoop集群总节点数据超过10000多个，存储容量超过170PB，活跃用户超过2000多。现在相关规模还在不断增长中，同时为了支持多元化需求，我们引入越来越多样的数据存储和分析方案，比如Hive、MapReduec、Spark 和HBase等，随之带来的管理和监控的挑战越来越严峻，数据安全问题亦是其中最重要的之一。

大数据时代，安全问题开始变得空前的关键，特别eBay作为全球领先的电子商务公司，我们必须保证Hadoop中用户数据的绝对安全。通常我们的安全措施根据如下几点 ：访问控制、安全隔离、数据分类、数据加密以及实时数据行为监控，然而经过广泛的尝试和研究，我们意识到没有任何已经存在的产品或者解决方案能够充分满足我们面临海量实时数据流和多元化用例场景下数据行为监控的需求。为了逾越这道鸿沟，eBay决定从头开始构建Eagle。

![](/images/logo_700x400.png)

> “Eagle 是开源分布式实时Hadoop数据安全方案，支持数据行为实时监控，能立即监测出对敏感数据的访问或恶意的操作，并立即采取应对的措施”

我们相信Eagle将成为Hadoop数据安全领域的核心组件之一，因此我们决定将它的功能分享给整个社区。目前我们已经将Eagle捐赠给Apache软件基金会作为Apache 孵化器项目开源，期望能够同开源社区一同协作开发，使得Eagle不断发展壮大，共同满足开源社区中更广泛的需求。

Eagle的数据行为监控方案可用于如下几类典型场景：

* 监控Hadoop中的数据访问流量
* 检测非法入侵和违反安全规则的行为
* 检测并防止敏感数据丢失和访问
* 实现基于策略的实时检测和预警
* 实现基于用户行为模式的异常数据行为检测

Eagle具有如下特点：

* **高实时**： 我们充分理解安全监控中高度实时和快速反应的重要性，因此设计Eagle之初，我们竭尽可能地确保能在亚秒级别时间内产生告警，一旦综合多种因素确订为危险操作，立即采取措施阻止非法行为。
* **可伸缩**：在eBay Eagle 被部署在多个大型Hadoop集群上，这些集群拥有数百PB的数据，每天有8亿以上的数据访问时间，因此Eagle必须具有处理海量实时数据的高度可伸缩能力。
* **简单易用**：可用性也是Eagle产品的核心设计原则之一。通过Eagle的Sandbox，使用者仅需数分钟便可以设置好环境并开始尝试。为了使得用户体验尽可能简单，我们内置了许多很好的例子，只需简单地点击几步鼠标，便可以轻松地完成策略地创建和添加。
* **用户Profile**：Eagle 内置提供基于机器学习算法对Hadoop中用户行为习惯建立用户Profile的功能。我们提供多种默认的机器学习算法供你选择用于针对不同HDFS特征集进行建模，通过历史行为模型，Eagle可以实时地检测异常用户行为并产生预警。
* **开源**：Eagle一直根据开源的标准开发，并构建于诸多大数据领域的开源产品之上，因此我们决定以Apache许可证开源Eagle，以回馈社区，同时也期待获得社区的反馈、协作与支持。


## Eagle概览

![](/images/posts/eagle-group.png)

#### 数据流接入和存储（Data Collection and Storage）
Eagle提供高度可扩展的编程API，可以支持将任何类型的数据源集成到Eagle的策略执行引擎中。例如，在Eagle HDFS 审计事件（Audit）监控模块中，通过Kafka来实时接收来自Namenode Log4j Appender 或者 Logstash Agent 收集的数据；在Eagle Hive 监控模块中，通过YARN API 收集正在运行Job的Hive 查询日志，并保证比较高的可伸缩性和容错性。

#### 数据实时处理（Data Processing）

**流处理API（Stream Processing API）Eagle** 提供独立于物理平台而高度抽象的流处理API，目前默认支持Apache Storm，但是也允许扩展到其他任意流处理引擎，比如Flink 或者 Samza等。该层抽象允许开发者在定义监控数据处理逻辑时，无需在物理执行层绑定任何特定流处理平台，而只需通过复用、拼接和组装例如数据转换、过滤、外部数据Join等组件，以实现满足需求的DAG（有向无环图），同时，开发者也可以很容易地以编程地方式将业务逻辑流程和Eagle 策略引擎框架集成起来。Eagle框架内部会将描述业务逻辑的DAG编译成底层流处理架构的原生应用，例如Apache Storm Topology 等，从事实现平台的独立。

__以下是一个Eagle如何处理事件和告警的示例：__

	StormExecutionEnvironment env = ExecutionEnvironmentFactory.getStorm(config); // storm env
	StreamProducer producer = env.newSource(new KafkaSourcedSpoutProvider().getSpout(config)).renameOutputFields(1) // declare kafka source
	       .flatMap(new AuditLogTransformer()) // transform event
	       .groupBy(Arrays.asList(0))  // group by 1st field
	       .flatMap(new UserProfileAggregatorExecutor()); // aggregate one-hour data by user
	       .alertWithConsumer(“userActivity“,”userProfileExecutor“) // ML policy evaluation
	env.execute(); // execute stream processing and alert

**告警框架（Alerting Framework）Eagle**告警框架由流元数据API、策略引擎服务提供API、策略Partitioner API 以及预警去重框架等组成:

* **流元数据API** 允许用户声明事件的Schema，包括事件由哪些属性构成、每个属性的类型，以及当用户配置策略时如何在运行时动态解析属性的值等。
* **策略引擎服务提供API** 允许开发者很容易地以插件的形式扩展新的策略引擎。WSO2 Siddhi CEP 引擎是Eagle 优先默认支持的策略引擎，同时机器学习算法也可作为另一种策略引擎执行。
* **扩展性** Eagle的策略引擎服务提供API允许你插入新的策略引擎

		public interface PolicyEvaluatorServiceProvider {
		  public String getPolicyType();         // literal string to identify one type of policy
		  public Class<? extends PolicyEvaluator> getPolicyEvaluator(); // get policy evaluator implementation
		  public List<Module> getBindingModules();  // policy text with json format to object mapping
		}
		public interface PolicyEvaluator {
		  public void evaluate(ValuesArray input) throws Exception;  // evaluate input event
		  public void onPolicyUpdate(AlertDefinitionAPIEntity newAlertDef); // invoked when policy is updated
		  public void onPolicyDelete(); // invoked when policy is deleted
		}

* **策略Partitioner API** 允许策略在不同的物理节点上并行执行。也允许你自定义策略Partitioner类。这些功能使得策略和事件完全以分布式的方式执行。
* **可伸缩性** Eagle 通过支持策略的分区接口来实现大量的策略可伸缩并发地运行

		public interface PolicyPartitioner extends Serializable {
		  int partition(int numTotalPartitions, String policyType, String policyId); // method to distribute policies
		}



	![](/images/posts/policy-partition.png)

	> 可伸缩的Eagle策略执行框架

**机器学习模块:**
Eagle 支持根据用户在Hadoop平台上历史使用行为习惯来定义行为模式或用户Profile的能力。拥有了这个功能，不需要在系统中预先设置固定临界值的情况下，也可以实现智能地检测出异常的行为。Eagle中用户Profile是通过机器学习算法生成，用于在用户当前实时行为模式与其对应的历史模型模式存在一定程度的差异时识别用户行为是否为异常。目前，Eagle 内置提供以下两种算法来检测异常，分别为特征值分解（Eigen-Value Decomposition）和 密度估计（Density Estimation）。这些算法从HDFS 审计日志中读取数据，对数据进行分割、审查、交叉分析，周期性地为每个用户依次创建Profile 行为模型。一旦模型生成，Eagle的实时流策略引擎能够近乎实时地识别出异常，分辨当前用户的行为可疑的或者与他们的历史行为模型不相符。

下图简单描述了目前Eagle中用户Profile的离线训练建模和在线实时监测的数据流：

![](/images/posts/ml-pipeline.png)

> 用户Profile 离线训练以及异常监测架构

基于用户 Profile的Eagle在线实时异常监测是根据Eagle的通用策略框架实现的，用户Profile只是被定义为Eagle系统中一个策略而已，用户Profile的策略是通过继承自Eagle统一策略执行接口的机器学习Evaluator来执行，其策略的定义中包括异常检测过程中需要的特征向量等（在线检测与离线训练保持一致）。

此外，Eagle 提供自动训练调度器，可根据文件或者UI配置的时间周期和粒度来调度这个基于Spark的离线训练程序，用于批量创建用户Profile和行为模型，默认该训练系统以每月的频率更新模型，模型粒度为一分钟。

Eagle 内置的机器学习算法基本思想如下：

**核密度估计算法 (Density Estimation)**
该算法的基本思想是根据检测的训练样本数据针对每个用户计算出对应的概率密度分布函数。首先，我们对训练数据集的每个特征均值标准化，标准化可以使得所有数据集转化为相同尺度。然后，在我们的随机变量概率分布估计中，我们采用高斯分布式函数来计算概率密度。假设任意特征彼此相互独立，那么最终的高斯概率密度就可以通过分解各个特征的概率密度而计算得到。在线实时检测阶段，我们可以首先计算出每个用户实时行为的概率。如果用户出现当前行为的可能性低于某个临界值，我们表识为异常警告，而这个临界值完全由离线训练程序通过称为“马修斯相关系数”（Mathews Correlation Coefficient）的方法计算而得。

![](/images/posts/density-estimation.png)

> 展示单一维度上用户行为直方图

**特征值分解算法（Eigen-Value Decomposition）**
该算法中，我们认为生成用户Profile的主要目的是为了从中发现有价值的用户行为模式。为了实现这个目的，我们可以考虑对特征依次进行组合，然后观察他们相互之间是如何影响的。当数据集非常巨大时，正如通常我们所遇到的场景，由于正常模式的数量非常之多，以至于特征集的异常的模式很容易被忽视。由于正常的行为模式通常处于非常低维的子空间内，因此我们也许可以通过降低数据集的维度来更好的理解用户的真正的行为模式。该方法同样可以对于训练数据集进行降噪。根据对大量用户特征数据方差的进行运算，通常在我们的用例场景中选取方差为95%作为基准，我们可以得到方差为95%的主成分的数量为k，因此我们将前k个主成分认为是用户的正常子空间，而剩下的(n-k)个主成分则被视为异常子空间。

当线实时异常检测时，如果用户行为模式位于正常子空间附近，则认为该行为正常，否则，如果用户行为模式位于异常子空间附近，则会立即报警，因为我们相信通常用户行为一般应该位于正常子空间内。至于如何计算用户当前行为接近正常还是异常子空间，我们采用的是欧氏距离法（Euclidian distance method）。

![](/images/posts/eigen-decomposition.png)

> 展示重要的用户行为模式成分

**Eagle 服务**

**策略管理器** Eagle策略管理器提供交互友好的用户界面和REST API 供用户轻松地定义和管理策略，一切只需几次鼠标点击而已。Eagle的用户界面使得策略的管理、敏感元数据的标识和导入、HDFS或Hive 的资源浏览以及预警仪表等功能都非常易于使用。

Eagle 策略引擎默认支持WSO2的Siddhi CEP引擎和机器学习引擎，以下是几个基于Siddi CEP的策略示例

* 单一事件执行策略（用户访问Hive中的敏感数据列）

		from hiveAccessLogStream[sensitivityType=='PHONE_NUMBER'] select * insert into outputStream;

* 基于窗口的策略（用户在10分钟内访问目录 /tmp/private 多余 5次）

		hdfsAuditLogEventStream[(src == '/tmp/private')]#window.externalTime(timestamp,10 min) select user, count(timestamp) as aggValue group by user having aggValue >= 5 insert into outputStream;

**查询服务（Query Service）** Eagle 提供类SQL的REST API用来实现针对海量数据集的综合计算、查询和分析的能力，支持例如过滤、聚合、直方运算、排序、top、算术表达式以及分页等。Eagle优先支持HBase 作为其默认数据存储，但是同时也支持基JDBC的关系型数据库。特别是当选择以HBase作为存储时，Eagle便原生拥有了HBase存储和查询海量监控数据的能力，Eagle 查询框架会将用户提供的类SQL查询语法最终编译成为HBase 原生的Filter 对象，并支持通过HBase Coprocessor进一步提升响应速度。

	query=AlertDefinitionService[@dataSource="hiveQueryLog"]{@policyDef}&pageSize=100000

## Eagle在eBay的使用场景
目前，Eagle的数据行为监控系统已经部署到一个拥有2500多个节点的Hadoop集群之上，用以保护数百PB数据的安全，并正计划于今年年底之前扩展到其他上十个Hadoop集群上，从而覆盖eBay 所有主要Hadoop的10000多台节点。在我们的生产环境中，我们已针对HDFS、Hive 等集群中的数据配置了一些基础的安全策略，并将于年底之前不断引入更多的策略，以确保重要数据的绝对安全。目前，Eagle的策略涵盖多种模式，包括从访问模式、频繁访问数据集，预定义查询类型、Hive 表和列、HBase 表以及基于机器学习模型生成的用户Profile相关的所有策略等。同时，我们也有广泛的策略来防止数据的丢失、数据被拷贝到不安全地点、敏感数据被未授权区域访问等。Eagle策略定义上极大的灵活性和扩展性使得我们未来可以轻易地继续扩展更多更复杂的策略以支持更多多元化的用例场景。

## 后续计划
过去两年中，在eBay 除了被用于数据行为监控以外，Eagle 核心框架还被广泛用于监控节点健康状况、Hadoop应用性能指标、Hadoop 核心服务以及整个Hadoop集群的健康状况等诸多领域。我们还建立一系列的自动化机制，例如节点修复等，帮助我们平台部门极大得节省了我们人工劳力，并有效地提升了整个集群资源地利用率。

以下是我们目前正在开发中地一些特性：

* 扩展机器学习模型对Hive和HBase支持
* 提供高度可扩展的API，以方便集目前业界广泛使用的其他监控预警平台或者工具，如Ganglia和Nagios等，同时支持敏感数据的导入，如与Dataguise 集成等。
* 此外，我们正在积极整理其他Hadoop 集群监控模块，期望在后续发布中开源给社区，例如
	* HBase 监控
	* Hadoop 作业性能监控
	* Hadoop 节点监控

## 关于作者
[陈浩](https://github.com/haoch)，Apache Eagle Committer 和 PMC 成员，eBay 分析平台基础架构部门高级软件工程师，负责Eagle的产品设计、技术架构、核心实现以及开源社区推广等。

感谢以下来自Apache Eagle社区和eBay公司的联合作者们对本文的贡献：

* [张勇](https://github.com/yonzhang)，Apache Eagle Committer和PMC，eBay 资深架构师
* [孙立斌](https://github.com/sunlibin)，Apache Eagle Committer和PMC，eBay 软件工程师
* [蒋吉麟](https://github.com/zombiej)，Apache Eagle Committer和PMC，eBay 软件工程师
* [赵晴雯](https://github.com/qingwen220)，Apache Eagle Committer和PMC，eBay 软件工程师

eBay 分析平台基础架构部（Analytics Data Infrastructure）是eBay的全球数据及分析基础架构部门，负责eBay在数据库、数据仓库、Hadoop、商务智能以及机器学习等各个数据平台开发、管理等,支持eBay全球各部门运用高端的数据分析解决方案作出及时有效的作业决策，为遍布全球的业务用户提供数据分析解决方案。

## 参考资料

* Apache Eagle 文档：[http://goeagle.io](http://goeagle.io)
* Apache Eagle 源码：[http://github.com/ebay/eagle](http://github.com/ebay/eagle)
* Apache Eagle 项目：[http://incubator.apache.org/projects/eagle.html](http://incubator.apache.org/projects/eagle.html)

## 引用链接
* **CSDN**: [http://www.csdn.net/article/2015-10-29/2826076](http://www.csdn.net/article/2015-10-29/2826076)
* **OSCHINA**: [http://www.oschina.net/news/67515/apache-eagle](http://www.oschina.net/news/67515/apache-eagle)
* **China Hadoop Summit**: [http://mp.weixin.qq.com/s?...](http://mp.weixin.qq.com/s?__biz=MzA4MTkyODIzMA==&mid=400298495&idx=1&sn=954031ba8065481c31a3464e2c8a26a5&scene=1&srcid=1102zgGQzedckCmNrfRwounA&uin=MjYyNzgwNDQwMA%3D%3D&key=04dce534b3b035efe14d53fcf6e7062a63179003551e59fad5cf8584703fcaa38779cc4c93cbf931c25f6b34cb2d7653&devicetype=iMac+MacBookPro11%2C3+OSX+OSX+10.10.5+build(14F1021)&version=11020201&lang=en&pass_ticket=TC%2Bod2ZeFnhmci%2Bi4%2BxTTVD6moUrNFX8RXppzoQSa%2BXO3C7evUDs6njeYbsYyCFD)
* **Apache Kylin**: [http://mp.weixin.qq.com/s?...](http://mp.weixin.qq.com/s?__biz=MzAwODE3ODU5MA==&mid=400287781&idx=1&sn=343b2b29a37f8ed53a7ecb0465faf515&scene=0&uin=MjYyNzgwNDQwMA%3D%3D&key=04dce534b3b035ef73f964362ac4c43d452ab1b208eb357c488dfcd7d69209e060cfe01e9b146752517d2096f6751370&devicetype=iMac+MacBookPro11%2C3+OSX+OSX+10.10.5+build(14F1021)&version=11020201&lang=en&pass_ticket=TC%2Bod2ZeFnhmci%2Bi4%2BxTTVD6moUrNFX8RXppzoQSa%2BXO3C7evUDs6njeYbsYyCFD)


<hr />

_本文来自Apache Eagle网站：[http://goeagle.io](http://goeagle.io)，转载请注明出处和来源。_
