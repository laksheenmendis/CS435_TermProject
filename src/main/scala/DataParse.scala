
import org.apache.spark._

object DataParse {

  def main(args: Array[String]) : Unit = {
//    val conf = new SparkConf().setAppName("censusAnalysis").setMaster("local")
    val conf = new SparkConf().setAppName("censusAnalysis").setMaster("yarn")
    val sc = new SparkContext(conf)

    //Getting all of the incident data that is needed for analysis
//    val incidentFile1 = sc.textFile("data/testingData/incidents1")
    val incidentFile1 = sc.textFile(args(0))
//    val incidentFile2 = sc.textFile("data/testingData/incidents2.txt")
    val incidentFile2 = sc.textFile(args(1))

    val incidentData1 = incidentFile1.map(value => {
      val result = value.substring(1,value.length-1)
      val data = result.split(",")

      //zip code -- time, incident category, police district
      (data(0), data(3)+";"+data(15)+";"+data(21))
    })
    val incidentData2 = incidentFile2.map(value => {
      val result = value.substring(1,value.length-1)
      val data = result.split(",")

      //zip code -- time, incident category, police district
      (data(0), data(3)+";"+data(15)+";"+data(21))
    })

    //merging the incident data
    val incidents = incidentData1.union(incidentData2)

    //Getting all of the census data that is needed for analysis
//    val censusFile1 = sc.textFile("data/censusData1")
    val censusFile1 = sc.textFile(args(2))
//    val censusFile2 = sc.textFile("data/censusData2")
    val censusFile2 = sc.textFile(args(3))

    val censusData1 = censusFile1.map(value => {
      val result = value.substring(8,value.length-2)
      val data = result.split(",")

      //zip code -- Total Population, aggregate income
      (value.substring(1,6), data(0)+";"+data(1237)+";"+data(1504))
    })

    val censusData2 = censusFile2.map(value => {
      val result = value.substring(8,value.length-2)
      val data = result.split(",")

      //zip code -- Total Population, aggregate income
      (value.substring(1,6), data(0)+";"+data(1237)+";"+data(1504))
    })

    //Remove duplicate values
    val census = censusData1.union(censusData2).reduceByKey((value1, value2) => value1)

    //Join the census and the incidents, key is zip code
    val joinedData = census.join(incidents)

    //Doing some intermediate processing. Making the data easier to parse
    val intermediate = joinedData.reduceByKey((value1, value2) => {
      val censusPortion = value1._1

      val result = value1._2 + "--" + value2._2

      (censusPortion,result)
    })

    //Getting the final data: zip code key, then census data, and overall incident report data
    val finalData = intermediate.map(value => {
      val censusPortion = "censusData: {" + value._2._1 + "}"

      var num1 = 0
      var num2 = 0
      var num3 = 0
      var num4 = 0
      //Getting the number of incidents for each zip code, organized by 6 hour time slots
      val incidents = value._2._2.split("--")
      var index = 0
      while(index != incidents.length) {
        val incidentArray = incidents(index).split(";")
        val firstHalf = incidentArray(0).substring(0,2).toDouble

        if(0 <= firstHalf && firstHalf < 6){
          num1 = num1 + 1
        }
        else if(6 <= firstHalf && firstHalf < 12){
          num2 = num2 + 1
        }
        else if(12 <= firstHalf && firstHalf < 18){
          num3 = num3 + 1
        }
        else if(18 <= firstHalf && firstHalf < 24){
          num4 = num4 + 1
        }
        index = index + 1
      }

      val time1 = "incidentData(hour 0 - 6): {" + num1 + "}"

      val time2 = "incidentData(hour 6 - 12): {" + num2 + "}"

      val time3 = "incidentData(hour 12 - 18): {" + num3 + "}"

      val time4 = "incidentData(hour 18 - 24): {" + num4 + "}"
      val result = time1 + "----" + time2 + "----" + time3 + "----" + time4

      (value._1, (censusPortion, result))
    })

//    finalData.saveAsTextFile("output")
    finalData.saveAsTextFile(args(4))
  }
}
