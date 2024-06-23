import com.amazonaws.services.glue.util.{GlueArgParser, Job}
import com.amazonaws.services.glue.{DynamicFrame, GlueContext}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object GlueApp {
  def main(sysArgs: Array[String]): Unit = {
    val args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME").toArray)

    // initialize spark and glue contexts
    val sc: SparkContext = new SparkContext()
    val glueContext: GlueContext = new GlueContext(sc)
    val spark: SparkSession = glueContext.getSparkSession

    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

    Job.init(args("JOB_NAME"), glueContext)

    val dyf: DynamicFrame = glueContext.getCatalogSource(database = "capitol-trades", tableName = "trades").getDynamicFrame()

    val df = dyf.toDF()

    // transformations
    val transformedDF = df.select(df.columns.map(c => when(col(c) === "N/A", null).otherwise(col(c)).alias(c)): _*) // replace "N/A"
        .withColumn("filed_after", regexp_replace(col("filed_after"), "days", "").cast("int"))
        .withColumn("price", col("price").cast("float"))
        .withColumn("traded_date", to_date(col("traded_date"), "yyyy d MMM"))
        .withColumn("published_date", to_date(col("published_date"), "yyyy d MMM"))
        .withColumn("politician_first_name", split(col("politician"), " ").getItem(0))
        .withColumn("politician_last_name", split(col("politician"), " ").getItem(1))

    // write to s3
    val outputPath = "s3://capitol-trades-data-lake/trades_parquet/"
    transformedDF.write.mode("overwrite").partitionBy("state").parquet(outputPath)

    // commit job
    Job.commit()
  }
}
