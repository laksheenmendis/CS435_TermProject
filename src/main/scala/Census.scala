
import org.apache.spark._

object Census {

  def main(args: Array[String]) : Unit = {
//    val conf = new SparkConf().setAppName("censusAnalysis").setMaster("local")
    val conf = new SparkConf().setAppName("censusAnalysis").setMaster("yarn")
    val sc = new SparkContext(conf)

    //This array holds all relevant zip codes for searching
    // val zipArray = sc.textFile("us-zip-code-latitude-and-longitude.csv").map(line => line.substring(0,5)).collect()
    val zipArray = sc.textFile(args(0)).map(line => line.substring(0,5)).collect()
    //Getting data from zip codes that fall inside of San Francisco
    //val geo = sc.textFile("cageo.uf3").filter(value => zipArray contains value.substring(160,165))
    val geo = sc.textFile(args(1)).filter(value => zipArray contains value.substring(160,165))

    //Pair the zip code with the logical record number
    val LRN = geo.map(line => line.substring(18,25)).collect()
    val ID = geo.map(line => (line.substring(18,25),line.substring(160,165)))


    val dataFile1 = sc.textFile(args(2) + "/ca00001.uf3")
    val initialData1 = dataFile1.filter(value => LRN contains value.substring(15,22))
    var organizedData = initialData1.map(line => (line.substring(15,22), line.substring(23,line.length)+","))

    for(index <- 2 to 76) {
      if(index < 10) {
        val dataFile = sc.textFile(args(2) + "/ca0000" + index + ".uf3")
        val initialData = dataFile.filter(value => LRN contains value.substring(15,22))
        val data = initialData.map(line => (line.substring(15,22), line.substring(23,line.length)+","))

        organizedData = organizedData.join(data).map(value => (value._1, value._2._1 + value._2._2))
      }
      else{
        val dataFile = sc.textFile(args(2) + "/ca000" + index + ".uf3")
        val initialData = dataFile.filter(value => LRN contains value.substring(15,22))
        val data = initialData.map(line => (line.substring(15,22), line.substring(23,line.length)+","))

        organizedData = organizedData.join(data).map(value => (value._1, value._2._1 + value._2._2))
      }
    }

    val finalData = ID.join(organizedData).map(value => (value._2._1, (value._1, value._2._2)))

    finalData.saveAsTextFile(args(3))
  }
}
