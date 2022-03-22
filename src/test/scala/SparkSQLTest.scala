import org.apache.spark.sql.SparkSession

object SparkSQLTest {
  def main(args: Array[String]): Unit = {
    // 构建 SparkSession 实例对象
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[2]")
      // 设置 Shuffle 分区数目
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
      //导入隐式转换
    import spark.implicits._
    val dataFrame = Seq(
      ("hadoop", 1), ("spark", 2), ("flink", 3)
    ).toDF("value", "count")
    dataFrame.show(10,truncate=false)
    //TODO：行转列
    val df = dataFrame.groupBy().pivot($"value").sum("count")
    df.show(10, truncate = false)
    spark.stop()  }

}
