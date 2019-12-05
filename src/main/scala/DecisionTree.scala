import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{OneHotEncoderEstimator, StringIndexer, VectorAssembler}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object DecisionTree {
  def main(args: Array[String]): Unit = {

    val INCIDENT_FILE_1 = args(0)
    val INCIDENT_FILE_2 = args(1)
    val NO_OF_CROSS_VALIDATION_FOLDS = args(2)
    val OUTPUT_FILE = args(3)
    val MODEL_OUTPUT_PATH = args(4)

    val spark: SparkSession = SparkSession.builder.master("yarn").getOrCreate
    val sc = spark.sparkContext

    val sqlContext = SparkSession.builder().master("yarn").getOrCreate().sqlContext

    val sb = new StringBuilder("DecisionTree\n")

    //Getting all of the incident data that is needed for analysis
    var incidentFile1 = sc.textFile(INCIDENT_FILE_1)
    val incidentFile2 = sc.textFile(INCIDENT_FILE_2)

    val header1 = incidentFile1.first()
    incidentFile1 = incidentFile1.filter(row => row != header1)

    val incidents = incidentFile1.union(incidentFile2)

    val total_count = incidents.count()
    sb.append("Total Records : " + total_count + "\n\n")

    // do some preprocessing on zipcode, date, time, year, dayofweek, incident code and incident category
    var incidentData1 = incidents.map(value => {
      val result = value.substring(1,value.length-1)
      val data = result.split(",")

      if( toInt(data(0)) != 0 && !data(2).equals("null") && !data(3).equals("null") && !data(4).equals("null") && !data(5).equals("null") && !data(14).equals("null") && !data(15).equals("null")
        && data(2) != null && data(3) != null && data(4) != null && data(5) != null && data(14) != null && data(15) != null)
      {
        val zipCode = data(0)
        val month = get_month(data(2))
        val date = get_date(data(2))
        val hour = get_hour(data(3))
        val minute = get_minute(data(3))
        val year = data(4)
        val dayOfWeek = data(5)
        val incidentCode = data(14)
        val weight = weighIncident(data(15))

        zipCode+","+month+","+date+","+hour+","+minute+","+year+","+dayOfWeek+","+incidentCode+","+weight
      }
      else
      {
        ("")
      }
    })

    //remove empty values from the RDD
    incidentData1 = incidentData1.filter(value => !value.isEmpty)

    incidentData1.foreach( value => println(value))

    val total_after_removing_null = incidentData1.count()
    sb.append("Total Records after removing null values :"+total_after_removing_null + "\n\n")
    sb.append("Ratio %.2f".format(total_after_removing_null.toDouble/total_count.toDouble * 100) + "%\n\n")

    val schema = dfSchema()
    val data = incidentData1.map(_.split(",").to[List]).map(row)
    val dataFrame = spark.createDataFrame(data, schema)

    dataFrame.show()

    // perform one hot encoding on zipcode, month, date, hour, minute, year, day_of_week and incident_code
    val indexer = new StringIndexer()
      .setInputCol("dayOfWeek")
      .setOutputCol("dayOfWeekIndex")

    val encoder = new OneHotEncoderEstimator()
      .setInputCols(Array(indexer.getOutputCol, "zipcode",  "month", "date",  "hour", "minute", "year", "incidentCode"))
      .setOutputCols(Array("dayOfWeekVec", "zipCodeVec", "monthVec", "dateVec",  "hourVec", "minuteVec", "yearVec", "incidentCodeVec"))

    // Set the input columns as the features we want to use
    val assembler = (new VectorAssembler()
      .setInputCols(encoder.getOutputCols)
      .setOutputCol("features"))

    // create a pipeline
    val pipelineDF = new Pipeline().setStages(Array(indexer, encoder, assembler))

    import sqlContext.implicits._
    val oneHotEncodedDF = pipelineDF.fit(dataFrame).transform(dataFrame).select($"label",$"features")

    // Splitting the data by create an array of the training and test data
    val Array(training, test) = oneHotEncodedDF.randomSplit(Array(0.7, 0.3), seed = 12345)

    // create the model
    val decisionTree = new DecisionTreeClassifier()

    val paramGrid = new ParamGridBuilder().
      addGrid(decisionTree.impurity, Array("entropy", "gini")).
      addGrid(decisionTree.maxDepth, Array(2,4)).
      addGrid(decisionTree.minInstancesPerNode, Array(5,10)).
      addGrid(decisionTree.maxBins, Array(3)).
      build()

    // create cross val object, define scoring metric
    val cv = new CrossValidator().
      setEstimator(decisionTree).
      setEvaluator(new MulticlassClassificationEvaluator().setMetricName("weightedRecall")).
      setEstimatorParamMaps(paramGrid).
      setNumFolds(NO_OF_CROSS_VALIDATION_FOLDS.toInt).
      setParallelism(2)

    // You can then treat this object as the model and use fit on it.
    val model = cv.fit(training)

    model.save(MODEL_OUTPUT_PATH)

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

    // Accuracy by label
    sb.append("Accuracy = " + mMetrics.accuracy +"\n\n")

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

  // create the structure for dataframe
  def dfSchema(): StructType =
    StructType(
      Seq(
        StructField(name = "zipcode", dataType = IntegerType, nullable = false),
        StructField(name = "month", dataType = IntegerType, nullable = false),
        StructField(name = "date", dataType = IntegerType, nullable = false),
        StructField(name = "hour", dataType = IntegerType, nullable = false),
        StructField(name = "minute", dataType = IntegerType, nullable = false),
        StructField(name = "year", dataType = IntegerType, nullable = false),
        StructField(name = "dayOfWeek", dataType = StringType, nullable = false),
        StructField(name = "incidentCode", dataType = IntegerType, nullable = false),
        StructField(name = "label", dataType = IntegerType, nullable = false)
      )
    )

  def row(line: List[String]): Row = Row(line(0).toInt, line(1).toInt, line(2).toInt, line(3).toInt, line(4).toInt, line(5).toInt, line(6), line(7).toInt, line(8).toInt)


  def weighIncident(incidentType:String) : Int = {

    if(incidentType=="Non-Criminal" || incidentType=="Other" || incidentType=="Other Miscellaneous" || incidentType=="Case Closure") {
      return 1
    }
    else if(incidentType=="Miscellaneous Investigation" || incidentType=="Fraud" || incidentType=="Forgery And Counterfeiting" || incidentType=="Warrant" ||
      incidentType=="Traffic Violation Arrest" || incidentType=="Gambling" || incidentType=="Civil Sidewalks" || incidentType=="Courtesy Report") {
      return 2
    }
    else if(incidentType=="Juvenile Offenses" || incidentType=="Lost Property" || incidentType=="Suspicious Occ" || incidentType=="Suspicious" ||
      incidentType=="Vandalism" || incidentType=="Recovered Vehicle") {
      return 3
    }
    else if(incidentType=="Vehicle Misplaced" || incidentType=="Vehicle Impounded") {
      return 5
    }
    else if(incidentType=="Disorderly Conduct" || incidentType=="Traffic Collision" || incidentType=="Fire Report" || incidentType=="Weapons Carrying Etc") {
      return 8
    }
    else if(incidentType=="Liquor Laws" || incidentType=="Drug Offense" || incidentType=="Drug Violation" || incidentType=="Embezzlement") {
      return 10
    }
    else if(incidentType=="Motor Vehicle Theft" || incidentType=="Stolen Property" || incidentType=="Robbery" || incidentType=="Motor Vehicle Theft?" ||
      incidentType=="Larceny Theft" || incidentType=="Burglary" || incidentType=="Malicious Mischief") {
      return 14
    }
    else if(incidentType=="Prostitution" || incidentType=="Other Offenses") {
      return 15
    }
    else if(incidentType=="Arson" || incidentType=="Offences Against The Family And Children" || incidentType=="Family Offense" ||
      incidentType=="Missing Person" || incidentType=="Weapons Offense" || incidentType=="Weapons Offence") {
      return 18
    }
    else if(incidentType=="Suicide" || incidentType=="Rape" || incidentType=="Assault" || incidentType=="Sex Offense" || incidentType=="Homicide" ||
      incidentType=="Human Trafficking (A), Commercial Sex Acts" || incidentType=="Human Trafficking, Commercial Sex Acts") {
      return 20
    }
    else{
      println(incidentType)
      return 0
    }
  }

  // function for creating month column
  def get_month(date:String) : Int = {
    date.substring(5, 7).toInt
  }

  // function for creating date column
  def get_date(date:String) : Int = {
    date.substring(8, 10).toInt
  }

  // function for creating hour column
  def get_hour(time:String) : Int = {
    time.substring(0,2).toInt
  }

  // function for creating minute column
  def get_minute(time:String) : Int = {
    time.substring(3,5).toInt
  }

  // function to convert year and incident_code columns to int
  def toInt(value:String) : Int = {
    value.toInt
  }
}
