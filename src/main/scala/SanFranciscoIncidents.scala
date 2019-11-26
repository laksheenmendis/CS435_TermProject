import org.apache.spark.sql.SparkSession

object SanFranciscoIncidents {

  def main(args: Array[String]): Unit = {

    //TODO uncomment below line and remove the other
//    val PATH_TO_INCIDENTS = "./data/Police_Department_Incident_Reports.csv"
    val PATH_TO_INCIDENTS = "./data/incidents_new.csv"

    val PATH_TO_ZIP_CODES = "./data/us-zip-code-latitude-and-longitude.csv"
    val OUTPUT_PATH = "./output"
    val ITERATIONS = 25
    val NUMBER_OF_TOP_PAGES = 10

    case class ZipCode(zipcode:Int, city:String, state:String, latitude:Float, longitude:Float, timezone:Int , daylightflag:Int, geopoint:String)

    case class Incident(datetime:String,date:String,time:String,year:Int,day_of_week:String,report_datetime:String,row_ID:Long,incident_ID:Int,incident_Number:Long,CAD_Number:Long,report_type_code:String,report_type_description:String,filed_online:Boolean,code:Int,category:String,subcategory:String,description:String,resolution:String,intersection:String,CNN:Int,police_district:String,analysis_neighborhood:String,supervisor_district:Int,latitude:Float,longitude:Float,point:String,SF_find_neighborhoods:Int,current_police_districts:Int,current_supervisor_districts:Int,analysis_neighborhoods:Int,HSOC_zones:Int,OWED_public_spaces:Int,central_market_tenderloin_boundary_polygon_updated:Int,parks_alliance_CPSI:String,ESNCAG_boundary_file:Int)

    val sc = SparkSession.builder().master("local").getOrCreate().sparkContext

    val sqlContext = SparkSession.builder().master("local").getOrCreate().sqlContext

    var zipcodeDF= sqlContext.read.format("csv").option("header", "true").option("inferSchema", "true").option("delimiter", ";").load(PATH_TO_ZIP_CODES)
    val header1 = zipcodeDF.first()
    zipcodeDF = zipcodeDF.filter(row => row != header1)
    zipcodeDF.show()

    var incidentDF = sqlContext.read.format("csv").option("header","true").option("inferSchema", "true").load(PATH_TO_INCIDENTS)
    val header2 = incidentDF.first()
    incidentDF = incidentDF.filter(row => row != header2)
    incidentDF.show()


    }
  }
