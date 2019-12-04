import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{OneHotEncoderEstimator, StringIndexer, VectorAssembler}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object IncidentClassification {
  def main(args: Array[String]): Unit = {

    val INCIDENT_FILE_1 = args(0)
    val INCIDENT_FILE_2 = args(1)
    val NO_OF_CROSS_VALIDATION_FOLDS = args(2)
    val OUTPUT_FILE = args(3)

    val sb = new StringBuilder("")

    val spark: SparkSession = SparkSession.builder.master("yarn").getOrCreate
    val sc = spark.sparkContext
    val sqlContext = SparkSession.builder().master("yarn").getOrCreate().sqlContext

    //read in incident files which have zip codes
    var incidentDF1 = sqlContext.read.format("csv").option("header","false").load(INCIDENT_FILE_1)
    val header1 = incidentDF1.first()
    incidentDF1 = incidentDF1.filter(row => row != header1)

    val incidentDF2 = sqlContext.read.format("csv").option("header","false").load(INCIDENT_FILE_2)

    val incidents = incidentDF1.union(incidentDF2)

    val total_count = incidents.count()
    sb.append("Total Records : " + total_count + "\n\n")

    //select the required columns for the dataframe
    val columnNames = Seq("_c0", "_c2", "_c3", "_c4", "_c5", "_c14", "_c15")
    var filteredDF = incidents.select(columnNames.head, columnNames.tail: _*)

    // rename columns
    filteredDF = filteredDF.withColumnRenamed("_c0","zipcode_with_bracket")
      .withColumnRenamed("_c2","full_date")
      .withColumnRenamed("_c3","full_time")
      .withColumnRenamed("_c4","year")
      .withColumnRenamed("_c5","day_of_week")
      .withColumnRenamed("_c14","incident_code")
      .withColumnRenamed("_c15","incident_category")

//    filteredDF.show()

    val remove_bracket = udf((zipcodeVal:String) => zipcodeVal.substring(1, zipcodeVal.length).toInt)

    filteredDF = filteredDF.withColumn("zipcode", remove_bracket(filteredDF.col("zipcode_with_bracket")))

    val weight_incident = udf((incidentType:String) => {
      if(incidentType=="Non-Criminal" || incidentType=="Other" || incidentType=="Other Miscellaneous" || incidentType=="Case Closure")
        1
      else if(incidentType=="Miscellaneous Investigation" || incidentType=="Fraud" || incidentType=="Forgery And Counterfeiting" || incidentType=="Warrant" ||
        incidentType=="Traffic Violation Arrest" || incidentType=="Gambling" || incidentType=="Civil Sidewalks" || incidentType=="Courtesy Report")
        2
      else if(incidentType=="Juvenile Offenses" || incidentType=="Lost Property" || incidentType=="Suspicious Occ" || incidentType=="Suspicious" ||
        incidentType=="Vandalism" || incidentType=="Recovered Vehicle")
        3
      else if(incidentType=="Vehicle Misplaced" || incidentType=="Vehicle Impounded")
        5
      else if(incidentType=="Disorderly Conduct" || incidentType=="Traffic Collision" || incidentType=="Fire Report" || incidentType=="Weapons Carrying Etc")
        8
      else if(incidentType=="Liquor Laws" || incidentType=="Drug Offense" || incidentType=="Drug Violation" || incidentType=="Embezzlement")
        10
      else if(incidentType=="Motor Vehicle Theft" || incidentType=="Stolen Property" || incidentType=="Robbery" || incidentType=="Motor Vehicle Theft?" ||
        incidentType=="Larceny Theft" || incidentType=="Burglary" || incidentType=="Malicious Mischief")
        14
      else if(incidentType=="Prostitution" || incidentType=="Other Offenses")
        15
      else if(incidentType=="Arson" || incidentType=="Offences Against The Family And Children" || incidentType=="Family Offense" ||
        incidentType=="Missing Person" || incidentType=="Weapons Offense" || incidentType=="Weapons Offence")
        18
      else if(incidentType=="Suicide" || incidentType=="Rape" || incidentType=="Assault" || incidentType=="Sex Offense" || incidentType=="Homicide" ||
        incidentType=="Human Trafficking (A), Commercial Sex Acts" || incidentType=="Human Trafficking, Commercial Sex Acts")
        20
      else
        0
    }
    )

    //add the label column to the dataframe (it will be weights) and select columns
    var weighted_DF = filteredDF.withColumn("label", weight_incident(filteredDF.col("incident_category")))
        .select("zipcode",  "full_date", "full_time", "year","day_of_week", "incident_code", "label")

    //remove records which have atleast one null value
    var dfWithoutNulls = weighted_DF.filter(col("zipcode").notEqual(0))
      .filter(col("day_of_week").notEqual("null"))
      .filter(col("full_date").notEqual("null"))
      .filter(col("full_time").notEqual("null"))
      .filter(col("year").notEqual("null"))
      .filter(col("incident_code").notEqual("null"))
      .filter(col("label").notEqual(0))

    val total_after_removing_null = dfWithoutNulls.count()
    sb.append("Total Records after removing null values :"+total_after_removing_null + "\n\n")
    sb.append("Ratio %.2f".format(total_after_removing_null.toDouble/total_count.toDouble * 100) + "%\n\n")

    // udf for creating month column
    val get_month = udf((date:String) => date.substring(5, 7).toInt)
    // udf for creating date column
    val get_date = udf((date:String) => date.substring(8, 10).toInt)
    // udf for creating hour column
    val get_hour = udf((time:String) => time.substring(0,2).toInt)
    // udf for creating minute column
    val get_minute = udf((time:String) => time.substring(3,5).toInt)
    // udf to convert year and incident_code columns to int
    val toInt    = udf[Int, String]( _.toInt)

    dfWithoutNulls = dfWithoutNulls.withColumn("month", get_month(dfWithoutNulls.col("full_date")))
      .withColumn("date", get_date(dfWithoutNulls.col("full_date")))
      .withColumn("hour", get_hour(dfWithoutNulls.col("full_time")))
      .withColumn("minute", get_minute(dfWithoutNulls.col("full_time")))
      .withColumn("intYear", toInt(dfWithoutNulls("year")))
      .drop("year").withColumnRenamed("intYear", "year")
      .withColumn("int_incident_code", toInt(dfWithoutNulls("incident_code")))
      .drop("incident_code").withColumnRenamed("int_incident_code", "incident_code")

    //select needed columns
    dfWithoutNulls = dfWithoutNulls.select("zipcode",  "month", "date",  "hour", "minute", "year","day_of_week", "incident_code", "label")

//    dfWithoutNulls.foreach(x => println(x))

    // perform one hot encoding on zipcode, month, date, hour, minute, year, day_of_week and incident_code
    val indexer = new StringIndexer()
      .setInputCol("day_of_week")
      .setOutputCol("day_of_week_index")

    val encoder = new OneHotEncoderEstimator()
      .setInputCols(Array(indexer.getOutputCol, "zipcode",  "month", "date",  "hour", "minute", "year", "incident_code"))
      .setOutputCols(Array("dayOfWeekVec", "zipCodeVec", "monthVec", "dateVec",  "hourVec", "minuteVec", "yearVec", "incidentCodeVec"))

    // Set the input columns as the features we want to use
    val assembler = (new VectorAssembler()
      .setInputCols(encoder.getOutputCols)
      .setOutputCol("features"))

    // create a pipeline
    val pipelineDF = new Pipeline().setStages(Array(indexer, encoder, assembler))

    import sqlContext.implicits._
    val oneHotEncodedDF = pipelineDF.fit(dfWithoutNulls).transform(dfWithoutNulls).select($"label",$"features")

    // Splitting the data by create an array of the training and test data
    val Array(training, test) = oneHotEncodedDF.randomSplit(Array(0.7, 0.3), seed = 12345)

    // create the model
    val rf = new RandomForestClassifier()

    // create the param grid
    val paramGrid = new ParamGridBuilder().
      addGrid(rf.numTrees, Array(5, 10, 20, 30, 40, 50)).
      addGrid(rf.impurity, Array("entropy", "gini")).
      addGrid(rf.maxDepth, Array(2, 3, 5, 8)).
      addGrid(rf.minInstancesPerNode, Array(5, 10, 15, 20)).
      addGrid(rf.maxBins, Array(5, 10, 20, 30)).
      build()

    // create cross val object, define scoring metric
    val cv = new CrossValidator().
      setEstimator(rf).
      setEvaluator(new MulticlassClassificationEvaluator().setMetricName("weightedRecall")).
      setEstimatorParamMaps(paramGrid).
      setNumFolds(NO_OF_CROSS_VALIDATION_FOLDS.toInt).
      setParallelism(2)

    // You can then treat this object as the model and use fit on it.
    val model = cv.fit(training)

    sb.append("Estimator : " + model.bestModel.extractParamMap() +"\n\n")

    val results = model.transform(test).select("features", "label", "prediction")

    val predictionAndLabels = results.
      select($"prediction",$"label").
      as[(Double, Double)].
      rdd

    // Instantiate a new metrics objects
    val bMetrics = new BinaryClassificationMetrics(predictionAndLabels)
    val mMetrics = new MulticlassMetrics(predictionAndLabels)
    val labels = mMetrics.labels

//    println("Labels are: " + labels)
//    sb.append("Labels are: " + labels + "\n\n")

    // Get the Confusion matrix
    sb.append("Confusion matrix: \n" + mMetrics.confusionMatrix + "\n\n")

    // Precision by label
    sb.append("Precision\n")
    labels.foreach { l =>
//      println(s"Precision($l) = " + mMetrics.precision(l))
      sb.append(s"Precision($l) = " + mMetrics.precision(l)+ "\n")
    }
    sb.append("\n")

    // Recall by label
    sb.append("Recall\n")
    labels.foreach { l =>
//      println(s"Recall($l) = " + mMetrics.recall(l))
      sb.append(s"Recall($l) = " + mMetrics.recall(l) + "\n")
    }
    sb.append("\n")

    // False positive rate by label
    sb.append("False positive rate\n")
    labels.foreach { l =>
//      println(s"FPR($l) = " + mMetrics.falsePositiveRate(l))
      sb.append(s"FPR($l) = " + mMetrics.falsePositiveRate(l) + "\n")
    }
    sb.append("\n")

    // F-measure by label
    sb.append("F1-Score\n")
    labels.foreach { l =>
//      println(s"F1-Score($l) = " + mMetrics.fMeasure(l))
      sb.append(s"F1-Score($l) = " + mMetrics.fMeasure(l) + "\n")
    }
    sb.append("\n")

    // PrintWriter
    import java.io._
    val pw = new PrintWriter(new File(OUTPUT_FILE ))

    pw.write(sb.toString())
    pw.close

  }

}
