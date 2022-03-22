package cn.itcast.spark.store.es

import cn.itcast.spark.config.ApplicationConfig
import cn.itcast.spark.utils.{SparkUtils, StreamingUtils}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.types.{StringType, StructType}

/**
 * StructuredStreaming 实时消费Kafka Topic中数据，存入到Elasticsearch索引中
 */
object RealTimeOrder2Es {
	
	def main(args: Array[String]): Unit = {
		
		// 1. 获取SparkSession实例对象
		val spark: SparkSession = SparkUtils.createSparkSession(this.getClass)
		import spark.implicits._
		
		// 2. 从KAFKA读取消费数据
		val kafkaStreamDF: DataFrame = spark
			.readStream
			.format("kafka")
			.option("kafka.bootstrap.servers", ApplicationConfig.KAFKA_BOOTSTRAP_SERVERS)
			.option("subscribe", ApplicationConfig.KAFKA_ETL_TOPIC)
			// 设置每批次消费数据最大值
			.option("maxOffsetsPerTrigger", ApplicationConfig.KAFKA_MAX_OFFSETS)
			.load()
		
		// 定义Schema类型
		// 数据格式：{"orderId":"20200922170109216000002","userId":"500000066","orderTime":"2020-09-22 17:01:09.216","ip":"171.11.67.141","orderMoney":"281.49","orderStatus":0,"province":"河南省","city":"焦作市"}
		val orderSchema: StructType = new StructType()
			.add("orderId", StringType, nullable = true)
			.add("userId", StringType, nullable = true)
			.add("orderTime", StringType, nullable = true)
			.add("ip", StringType, nullable = true)
			.add("orderMoney", StringType, nullable = true)
			.add("orderStatus", StringType, nullable = true)
			.add("province", StringType, nullable = true)
			.add("city", StringType, nullable = true)
		
		// 3. 获取订单记录数据
		val orderStreamDF: DataFrame = kafkaStreamDF
			// 获取消息Message
			.selectExpr("CAST(value AS STRING)")
			.as[String] // 将DataFrame转换为Dataset
			.filter(msg => null != msg && msg.trim.length > 0)
			// 提取字段，使用from_json函数
			.select(
			    from_json($"value", orderSchema).as("order")
		    )
			.select($"order.*")
		
		// 4. 将数据保存至Elasticsearch 索引中
		val query: StreamingQuery = orderStreamDF.writeStream
			// 设置追加模式Append
			.outputMode(OutputMode.Append())
			// 设置检查点目录
			.option("checkpointLocation", ApplicationConfig.STREAMING_ES_CKPT)
			.format("es")
			.option("es.nodes", ApplicationConfig.ES_NODES)
			.option("es.port", ApplicationConfig.ES_PORT)
			.option("es.index.auto.create", ApplicationConfig.ES_INDEX_AUTO_CREATE)
			.option("es.write.operation", ApplicationConfig.ES_WRITE_OPERATION)
			.option("es.mapping.id", ApplicationConfig.ES_MAPPING_ID)
			.start(ApplicationConfig.ES_INDEX_NAME)
		// TODO: 5. 通过扫描HDFS文件，优雅的关闭停止StreamingQuery
		StreamingUtils.stopStructuredStreaming(query, ApplicationConfig.STOP_ES_FILE)
	}
	
}
