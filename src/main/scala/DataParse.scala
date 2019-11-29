
import org.apache.spark._

object DataParse {

  def main(args: Array[String]) : Unit = {
    val conf = new SparkConf().setAppName("censusAnalysis").setMaster("local")
//    val conf = new SparkConf().setAppName("censusAnalysis").setMaster("yarn")
    val sc = new SparkContext(conf)


    val dataFile1 = sc.textFile("temp.txt")
//    val dataFile2 = sc.textFile(args(2))

    dataFile1.foreach(value => println(value));

    var organizedData1 = dataFile1.map(value => (value.substring(1,6), value.substring(8, value.length-2)))

    organizedData1.foreach(value => println(value))

    organizedData1 = organizedData1.map(value => (value._1, value._2))
  }
}
