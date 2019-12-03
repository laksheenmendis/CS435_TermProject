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
    var incidentFile1 = sc.textFile(INCIDENT_FILE_1)
    val incidentFile2 = sc.textFile(INCIDENT_FILE_2)

    var incidentDF1 = sqlContext.read.format("csv").option("header","false").load(INCIDENT_FILE_1)
    val header1 = incidentDF1.first()
    incidentDF1 = incidentDF1.filter(row => row != header1)

    val incidentDF2 = sqlContext.read.format("csv").option("header","false").load(INCIDENT_FILE_2)

    val incidents = incidentDF1.union(incidentDF2)

    val columnNames = Seq("_c0", "_c2", "_c3", "_c4", "_c5", "_c14", "_c15")
    var filteredDF = incidents.select(columnNames.head, columnNames.tail: _*)

    //TODO remove
    filteredDF.show()

    val remove_bracket = udf((zipcodeVal:String) => zipcodeVal.substring(1, zipcodeVal.length))

    filteredDF = filteredDF.withColumn("zipcode", remove_bracket(filteredDF.col("_c0")))

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
    var weighted_DF = filteredDF.withColumn("weight", weight_incident(filteredDF.col("_c15")))

    weighted_DF.foreach(y => println(y))

    //remove records which have atleast one null value
    weighted_DF = weighted_DF.filter(
        weighted_DF.col("_c5").isNotNull
        .or(weighted_DF.col("zipcode").notEqual("00000"))
        .or(weighted_DF.col("_c2").notEqual("null"))
        .or(weighted_DF.col("_c3").notEqual("null"))
        .or(weighted_DF.col("_c4").notEqual("null"))
        .or(weighted_DF.col("_c14").notEqual("null"))
        .or(weighted_DF.col("_c15").notEqual("null"))
    )

    weighted_DF = weighted_DF.select("zipcode",  "_c2", "_c3", "_c4","_c5", "_c14", "weight")
    weighted_DF.foreach(x => println(x))


  }

}
