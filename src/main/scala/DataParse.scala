
import org.apache.spark._

object DataParse {

  def main(args: Array[String]) : Unit = {
    val conf = new SparkConf().setAppName("censusAnalysis").setMaster("local")
//    val conf = new SparkConf().setAppName("censusAnalysis").setMaster("yarn")
    val sc = new SparkContext(conf)

    //Getting all of the incident data that is needed for analysis
    val incidentFile1 = sc.textFile("data/incidentData1")
    val incidentFile2 = sc.textFile("data/incidentData2")

    val incidentData1 = incidentFile1.map(value => {
      val result = value.substring(1,value.length-1)
      val data = result.split(",")

      //zip code -- time, incident category, police district
      (data(0), data(3)+","+data(15)+","+data(21))
    })
    val incidentData2 = incidentFile2.map(value => {
      val result = value.substring(1,value.length-1)
      val data = result.split(",")

      //zip code -- time, incident category, police district
      (data(0), data(3)+","+data(15)+","+data(21))
    })

    //Getting all of the census data that is needed for analysis
    val censusFile1 = sc.textFile("data/censusData1")
    val censusFile2 = sc.textFile("data/censusData2")

    val censusData1 = censusFile1.map(value => {
      val result = value.substring(8,value.length-2)
      val data = result.split(",")

      //zip code -- Total Population, aggregate income
      (value.substring(1,6), data(0)+","+data(1237)+","+data(1504))
    })

    val censusData2 = censusFile2.map(value => {
      val result = value.substring(8,value.length-2)
      val data = result.split(",")

      //zip code -- Total Population, aggregate income
      (value.substring(1,6), data(0)+","+data(1237)+","+data(1504))
    })





    val dataFile1 = sc.textFile("temp.txt")
    //    val dataFile2 = sc.textFile(args(2))
    dataFile1.foreach(value => println(value));

    var organizedData1 = dataFile1.map(value => (value.substring(1,6), value.substring(8, value.length-2)))

    organizedData1.foreach(value => println(value))

    organizedData1 = organizedData1.map(value => (value._1, value._2))


  }
}
