
import org.apache.spark._

object Incidents {

  def main(args: Array[String]) : Unit = {

    //val conf = new SparkConf().setAppName("incidentParsing").setMaster("local")
    val conf = new SparkConf().setAppName("incidentParsing").setMaster("yarn")
    val sc = new SparkContext(conf)

    //Reading files
    //val incidentsFile = sc.textFile("data/Police_Department_Incident_Reports.csv")
    //val zipCodeFile = sc.textFile("data/us-zip-code-latitude-and-longitude.csv")
    val incidentsFile = sc.textFile(args(0))
    val zipCodeFile = sc.textFile(args(1))

    //Get only the zip code and pair it with its corresponding centroid latitude and longitude
    val zipCodes = zipCodeFile.map(value => (value.substring(0,5), value.split(";")(3) + "," + value.split(";")(4)))

    var incidents = incidentsFile.map(value => value.replaceAll(",,", ",null,"))
    incidents = incidents.map(value => value.replaceAll(",,", ",null,"))

    incidents = incidents.map(value => {
      var check = false
      var index = 0
      var result = value
      while(index != value.length) {
        if(value.charAt(index) == '\"') {
          check = !check
        }

        if(check && value.charAt(index) == ',') {
          val a = result.substring(0,index)
          val b = result.substring(index+1,result.length)
          result = a + ";" + b
        }
        index = index + 1
      }

      result
    })
    val filteredZipCodes = zipCodes.filter(value => value._1 != "Zip;C").collect()

    val zipIncidents = incidents.map(incident => {
      val array = incident.split(",")
      var zipCode = "empty"
      var distance = 10000.0
      if(array(23) == "null" || array(24) == "null") {
        ("00000", incident)
      }
      else {
        if (array(23) != "Latitude") {
          filteredZipCodes.foreach(value => {
            val tempArray = value._2.split(",")
            val tempDistance = math.sqrt(math.pow((tempArray(0).toDouble - array(23).toDouble), 2) + math.pow((tempArray(1).toDouble - array(24).toDouble), 2))
            if (tempDistance < distance) {
              distance = tempDistance
              zipCode = value._1
            }
          })
        }
        (zipCode, incident)
      }
    })

    //zipIncidents.foreach(value => println(value))
    zipIncidents.saveAsTextFile(args(2))
  }
}
