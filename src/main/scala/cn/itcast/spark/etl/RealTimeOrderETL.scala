package cn.itcast.spark.etl

import cn.itcast.spark.config.ApplicationConfig
import cn.itcast.spark.utils.{SparkUtils, StreamingUtils}
import org.apache.spark.SparkFiles
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.types.IntegerType
import org.lionsoul.ip2region.{DataBlock, DbConfig, DbSearcher}

/**
 * 订单数据实时ETL：实时从Kafka Topic 消费数据，进行过滤转换ETL，将其发送Kafka Topic，以便实时处理
 *      TODO：基于StructuredStreaming实现，Kafka作为Source和Sink
 */
object RealTimeOrderETL {

	/**
	 * 对流式数据StreamDataFrame进行ETL过滤清洗转换操作
	 */
	def streamingProcess(streamDF: DataFrame): DataFrame = {
		val session: SparkSession = streamDF.sparkSession
		import session.implicits._

		/*
			1）、从Kafka获取消息Message，使用get_json_object函数提交订单数据字段；
			2）、过滤获取订单状态为打开（orderState == 0）订单数据；
			3）、自定义UDF函数：ip_to_location，调用第三方库ip2Region解析IP地址为省份和城市；
			4）、组合订单字段为struct类型，转换为json字符串；
		 */
		// 1）、从Kafka获取消息Message，使用get_json_object函数提交订单数据字段；
		val orderStreamDF: DataFrame = streamDF
			// 获取消息Message
			.selectExpr("CAST(value AS STRING)")
			.as[String] // 将DataFrame转换为Dataset
			.filter(msg => null != msg && msg.trim.length > 0)
			// 提交订单数据字段
			// {"orderId":"20200922160543604000002","userId":"200000048","orderTime":"2020-09-22 16:05:43.604","ip":"36.61.211.41","orderMoney":144.32,"orderStatus":0}
			.select(
				get_json_object($"value", "$.orderId").as("orderId"), //
				get_json_object($"value", "$.userId").as("userId"), //
				get_json_object($"value", "$.orderTime").as("orderTime"), //
				get_json_object($"value", "$.ip").as("ip"), //
				get_json_object($"value", "$.orderMoney").as("orderMoney"), //
				get_json_object($"value", "$.orderStatus").cast(IntegerType).as("orderStatus") //
			)

		// 2）、过滤获取订单状态为打开（orderState == 0）订单数据；
		val filterOrderStreamDF: Dataset[Row] = orderStreamDF.filter($"orderStatus" === 0)

		// 3）、自定义UDF函数：ip_to_location，调用第三方库ip2Region解析IP地址为省份和城市；
		session.sparkContext.addFile(ApplicationConfig.IPS_DATA_REGION_PATH)
		val ip_to_location: UserDefinedFunction = udf(
			(ipValue: String) => {
				// a. 创建DbSearch对象，传递字典数据
				val dbSearcher: DbSearcher = new DbSearcher(new DbConfig(), SparkFiles.get("ip2region.db"))
				// b. 传递IP地址，进行解析
				val dataBlock: DataBlock = dbSearcher.btreeSearch(ipValue)
				val region: String = dataBlock.getRegion
				// c. 提取省份和城市 ->  中国|0|北京|北京市|阿里云
				val Array(_, _, province, city, _) = region.split("\\|")
				// d. 封装省份和城市及IP地址至Region对象
				(province, city) // Tuple2(_1: Any, _2: Any) -> struct
			}
		)
		val locationStreamDF: DataFrame = filterOrderStreamDF
			// 使用udf函数，解析IP地址
			.withColumn("location", ip_to_location($"ip"))
			// 提取省份和城市
			.withColumn("province", $"location._1")
			.withColumn("city", $"location._2")

		// 4）、组合订单字段为struct类型，转换为json字符串
		val etlStreamDF: DataFrame = locationStreamDF
			.select(
				// 表示订单数据存储Kafka Topic中Key
				$"orderId".as("key"),
				// 将DataFrame数据保存至Kafka Topic中，Message所在的字段：value 名称
				to_json(
					struct(
						$"orderId", $"userId", $"orderTime", $"ip",
						$"orderMoney", $"orderStatus", $"province", $"city"
					)
				).as("value")
			)

		// 返回ETL数据
		etlStreamDF
	}

	def main(args: Array[String]): Unit = {
		// 1. 创建SparkSession实例对象
		val spark: SparkSession = SparkUtils.createSparkSession(this.getClass)
		import spark.implicits._

		// 2. 从Kafka Topic消费数据
		val kafkaStreamDF: DataFrame = spark
			.readStream
			.format("kafka")
			.option("kafka.bootstrap.servers", ApplicationConfig.KAFKA_BOOTSTRAP_SERVERS)
			.option("subscribe", ApplicationConfig.KAFKA_SOURCE_TOPICS)
			// 设置每批次消费数据最大值
			.option("maxOffsetsPerTrigger", ApplicationConfig.KAFKA_MAX_OFFSETS)
			.load()

		// 3. 对数据进行ETL操作，封装到方法中
		val etlStreamDF: DataFrame = streamingProcess(kafkaStreamDF)

		// 4. 将流式数据进行输出
		val query: StreamingQuery = etlStreamDF.writeStream
			// 设置查询名称
			.queryName("query-order-etl")
			// 对流式应用输出来说，设置输出模式
			.outputMode(OutputMode.Append())
			.format("kafka")
			.option("kafka.bootstrap.servers", ApplicationConfig.KAFKA_BOOTSTRAP_SERVERS)
			.option("topic", ApplicationConfig.KAFKA_ETL_TOPIC)
			// 设置检查点目录
			.option("checkpointLocation", ApplicationConfig.STREAMING_ETL_CKPT)
			// TODO: 由于ETL属于从Kafka读取数据，向Kafka写入数据，可以设置流处理引擎：Continues Processing
			//.trigger(Trigger.Continuous("1 second"))
			// 流式应用，需要启动start
			.start()
		// 5. 流式查询等待流式应用终止
		//query.awaitTermination()
		//query.stop()
		StreamingUtils.stopStructuredStreaming(query, ApplicationConfig.STOP_ETL_FILE)
	}

}
