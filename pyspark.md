## pyspark
### RDD
	* 弹性分布式数据集
### spark streaming
	* 负责spark数据处理。数据拆分和流处理。
### Graphx
	* 主要用于基于图的算法，例如pageRank等等。
### SparkR
	* 通过R语言来调用spark。
### Spark SQL
	* 交互式的大数据SQL语言。
### MLBase
	* spark机器学习库
### Tachyon
	* 分布式文件系统， 速度高于HDFS3000倍


## Spark: python or scala
### 谁更好？
	* 如果处理对象是流数据，scalar将会是更好的选择。它支持asynchromous programming。当使用dataframe进行工作的时候，两者没有什么区别。但是需要注意到udf的使用。
### 应该学习哪一个
	* python的语法和可读性更高。当时对于有编程经验的人来说，scala可能是更好的选择。
### 类型安全谁更高
	* 因为scala在编译的时候进行类型检查，所以运行的速度会更快。
### 高级用法
	* python有更多的库可以选择。
		
		pySpark_cheat_sheet: https://s3.amazonaws.com/assets.datacamp.com/blog_assets/PySpark_Cheat_Sheet_Python.pdf


## Spark APIs: RDD, Dataset and dataframe
### RDD
	* RDD是spark最基本的block，他是spark最初的API，所有更高的水平的APIs都需要被分解成基本的RDD对象。RDD就是python或者scala表达数据的最基本的object。
### DataFrames
	* 它是RDD之上的高水平抽象。他允许你使用query-language对数据进行操作。但是请记住它任然构建在RDD之上。
### Dataset
	* 它是有类型检查的DataFrame。

## spark dataframe 和pandas dataframe的区别
spark的dataframe是为了大数据处理进行了优化。pandas dataframe仅仅是为了在单机上使用。
可以使用一下的语句将spark的dataframe转化成pandas的dataframe
		
		df.toPandas()
note: 要注意到数据集的大小，防止内存溢出。

## RDD vs Transformations
RDD支持两个种类的operations：
	* transformation: 从存在的数据创建一个新的数据集（数据）。
	* action: 在数据集上运行操作后返回值给程序（操作）。
