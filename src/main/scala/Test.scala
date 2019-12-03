import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{OneHotEncoderEstimator, StringIndexer}
import org.apache.spark.sql.SparkSession

object Test {

  def main(args: Array[String]): Unit = {


    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example").master("local[1]")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
//    val df = spark.createDataFrame(Seq(
//      (0, 3),
//      (1, 2),
//      (2, 4),
//      (3, 3),
//      (4, 3),
//      (5, 4)
//    )).toDF("id", "category")
//
//    val indexer = new StringIndexer().setInputCol("category").setOutputCol("categoryIndex")
////    val indexed = indexer.transform(df)
//
//    val encoder = new OneHotEncoderEstimator().setInputCols(Array("categoryIndex")).setOutputCols(Array("categoryVec"))
//
//    val pipeline = new Pipeline().setStages(Array(indexer, encoder))
//
//    pipeline.fit(df).transform(df)
//
//    df.select("id", "categoryVec").show()

    var df = spark.createDataFrame(Seq((0, "a", 1), (1, "b", 2), (2, "c", 3), (3, "a", 4), (4, "a", 4), (5, "c", 3))).toDF("id", "category1", "category2")

    val indexer = new StringIndexer().setInputCol("category1").setOutputCol("category1Index")
    val encoder = new OneHotEncoderEstimator()
      .setInputCols(Array(indexer.getOutputCol, "category2"))
      .setOutputCols(Array("category1Vec", "category2Vec"))

    val pipeline = new Pipeline().setStages(Array(indexer, encoder))

    df = pipeline.fit(df).transform(df).select("id", "category1Vec", "category2Vec")

    df.show()

  }

}
