package cn.itcast.spark.store.redis

import cn.itcast.spark.config.ApplicationConfig
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}

object StructureRedis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[2]")
      //设置shuffle分区数目
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()

    //导入隐式转换
     import spark.implicits._
    //1 从kafka 读取数据 底层采用NEW Consumer API
    val inputStreamDF = spark.readStream
      .format("socket")
      .option("host", "node1.itcast.cn")
      .option("port", 9999)
      .load()
    //2 业务分析
    val resultStreamDF: DataFrame = inputStreamDF
      //转换为Dataset类型
      .as[String]
      //过滤数据
      .filter(line => null != line && line.trim.nonEmpty)
      //分割单词
      .flatMap(line => line.trim.split("\\s+"))
      //按照单词分组聚合
      .groupBy($"value").count()
    //设置Streaming 应用输出及启动
    val query: StreamingQuery = resultStreamDF
      .writeStream
      .outputMode(OutputMode.Update())
      .foreachBatch {
        (batchDF: DataFrame, batchId: Long) =>
          batchDF
            // 行转列
            .groupBy()
            .pivot($"value").sum("count")
            //添加一列
            .withColumn("type", lit("spark"))
            .write
            .mode(SaveMode.Append)
            .format("org.apache.spark.sql.redis")
            .option("host", ApplicationConfig.REDIS_HOST)
            .option("port", ApplicationConfig.REDIS_PORT)
            .option("dbNum", ApplicationConfig.REDIS_DB)
            .option("table", "wordcount")
            .option("key.column", "type")
            .save()
      }.start()
    query.awaitTermination()
  query.stop()

  }

}
