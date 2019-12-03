import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{OneHotEncoderEstimator, StringIndexer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object IncidentClassification {
  def main(args: Array[String]): Unit = {

    val INCIDENT_FILE_1 = args(0)
    val INCIDENT_FILE_2 = args(1)

    //TODO change master to yarn
    val spark: SparkSession = SparkSession.builder.master("local").getOrCreate
    val sc = spark.sparkContext
    val sqlContext = SparkSession.builder().master("local").getOrCreate().sqlContext

    //read in incident files which have zip codes
    var incidentDF1 = sqlContext.read.format("csv").option("header","false").load(INCIDENT_FILE_1)
    val header1 = incidentDF1.first()
    incidentDF1 = incidentDF1.filter(row => row != header1)

    val incidentDF2 = sqlContext.read.format("csv").option("header","false").load(INCIDENT_FILE_2)

    val incidents = incidentDF1.union(incidentDF2)

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

    //add the weights column to the dataframe
    var weighted_DF = filteredDF.withColumn("weight", weight_incident(filteredDF.col("incident_category")))

    //select needed columns
    weighted_DF = weighted_DF.select("zipcode",  "full_date", "full_time", "year","day_of_week", "incident_code", "weight")

    //remove records which have atleast one null value
    var dfWithoutNulls = weighted_DF.filter(col("zipcode").notEqual(0))
    dfWithoutNulls = dfWithoutNulls.filter(col("day_of_week").notEqual("null"))
    dfWithoutNulls = dfWithoutNulls.filter(col("full_date").notEqual("null"))
    dfWithoutNulls = dfWithoutNulls.filter(col("full_time").notEqual("null"))
    dfWithoutNulls = dfWithoutNulls.filter(col("year").notEqual("null"))
    dfWithoutNulls = dfWithoutNulls.filter(col("incident_code").notEqual("null"))
    dfWithoutNulls = dfWithoutNulls.filter(col("weight").notEqual(0))

    // create new column for month
    val get_month = udf((date:String) => date.substring(5, 7).toInt)
    dfWithoutNulls = dfWithoutNulls.withColumn("month", get_month(dfWithoutNulls.col("full_date")))

    // create new column for date
    val get_date = udf((date:String) => date.substring(8, 10).toInt)
    dfWithoutNulls = dfWithoutNulls.withColumn("date", get_date(dfWithoutNulls.col("full_date")))

    // create new column for hour
    val get_hour = udf((time:String) => time.substring(0,2).toInt)
    dfWithoutNulls = dfWithoutNulls.withColumn("hour", get_hour(dfWithoutNulls.col("full_time")))

    // create new column for minute
    val get_minute = udf((time:String) => time.substring(3,5).toInt)
    dfWithoutNulls = dfWithoutNulls.withColumn("minute", get_minute(dfWithoutNulls.col("full_time")))

    // convert year and incident_code columns to int
    val toInt    = udf[Int, String]( _.toInt)
    dfWithoutNulls = dfWithoutNulls.withColumn("intYear", toInt(dfWithoutNulls("year")))
      .drop("year").withColumnRenamed("intYear", "year")

    dfWithoutNulls = dfWithoutNulls.withColumn("int_incident_code", toInt(dfWithoutNulls("incident_code")))
      .drop("incident_code").withColumnRenamed("int_incident_code", "incident_code")

    //select needed columns
    dfWithoutNulls = dfWithoutNulls.select("zipcode",  "month", "date",  "hour", "minute", "year","day_of_week", "incident_code", "weight")

    dfWithoutNulls.foreach(x => println(x))

    // perform one hot encoding on zipcode, month, date, hour, minute, year, day_of_week and incident_code
    val indexer = new StringIndexer().setInputCol("day_of_week").setOutputCol("day_of_week_index")
    val encoder = new OneHotEncoderEstimator()
      .setInputCols(Array(indexer.getOutputCol, "zipcode",  "month", "date",  "hour", "minute", "year", "incident_code"))
      .setOutputCols(Array("dayOfWeekVec", "zipCodeVec", "monthVec", "dateVec",  "hourVec", "minuteVec", "yearVec", "incidentCode_Vec"))

    val pipeline = new Pipeline().setStages(Array(indexer, encoder))

    val oneHotEncodedDF = pipeline.fit(dfWithoutNulls).transform(dfWithoutNulls).select("zipCodeVec", "monthVec", "dateVec",  "hourVec", "minuteVec", "yearVec", "incidentCode_Vec", "dayOfWeekVec", "weight")

    oneHotEncodedDF.show()


  }

}
