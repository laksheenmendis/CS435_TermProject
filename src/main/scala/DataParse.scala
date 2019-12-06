
import org.apache.spark._

object DataParse {

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

      //zip code -- Total Population, aggregate household income, population with a poverty status
      (value.substring(1,6), "Total Population: "+data(1)+"; Median Household Income: "+data(1237)+"; Per Capita Income: "+data(1455)+"; Population with a Poverty Status "+data(1507))
    })

    val censusData2 = censusFile2.map(value => {
      val result = value.substring(8,value.length-2)
      val data = result.split(",")

      //zip code -- Total Population, aggregate household income, population with a poverty status
      (value.substring(1,6), "Total Population: "+data(1)+"; Median Household Income: "+data(1237)+"; Per Capita Income: "+data(1455)+"; Population with a Poverty Status "+data(1507))
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

      var theftCount1 = 0
      var theftCount2 = 0
      var theftCount3 = 0
      var theftCount4 = 0

      var time1Avg = 0;
      var time2Avg = 0;
      var time3Avg = 0;
      var time4Avg = 0;

      //Getting the number of incidents for each zip code, organized by 6 hour time slots
      val incidents = value._2._2.split("--")
      var index = 0
      while(index != incidents.length) {
        val incidentArray = incidents(index).split(";")
        val firstHalf = incidentArray(0).substring(0,2).toDouble
        val incidentType = incidentArray(1)
        val weight = weighIncident(incidentType)

        if(0 <= firstHalf && firstHalf < 6){
          num1 = num1 + 1
          time1Avg = time1Avg + weight
          if(incidentType=="Motor Vehicle Theft" || incidentType=="Stolen Property" || incidentType=="Robbery" || incidentType=="Motor Vehicle Theft?" ||
            incidentType=="Larceny Theft" || incidentType=="Burglary") {
            theftCount1 = theftCount1 + 1
          }
        }
        else if(6 <= firstHalf && firstHalf < 12){
          num2 = num2 + 1
          time2Avg = time2Avg + weight
          if(incidentType=="Motor Vehicle Theft" || incidentType=="Stolen Property" || incidentType=="Robbery" || incidentType=="Motor Vehicle Theft?" ||
            incidentType=="Larceny Theft" || incidentType=="Burglary") {
            theftCount2 = theftCount2 + 1
          }
        }
        else if(12 <= firstHalf && firstHalf < 18){
          num3 = num3 + 1
          time3Avg = time3Avg + weight
          if(incidentType=="Motor Vehicle Theft" || incidentType=="Stolen Property" || incidentType=="Robbery" || incidentType=="Motor Vehicle Theft?" ||
            incidentType=="Larceny Theft" || incidentType=="Burglary") {
            theftCount3 = theftCount3 + 1
          }
        }
        else if(18 <= firstHalf && firstHalf < 24){
          num4 = num4 + 1
          time4Avg = time4Avg + weight
          if(incidentType=="Motor Vehicle Theft" || incidentType=="Stolen Property" || incidentType=="Robbery" || incidentType=="Motor Vehicle Theft?" ||
            incidentType=="Larceny Theft" || incidentType=="Burglary") {
            theftCount4 = theftCount4 + 1
          }
        }
        index = index + 1
      }

      val average1 = time1Avg.toFloat / num1;
      val average2 = time2Avg.toFloat / num2;
      val average3 = time3Avg.toFloat / num3;
      val average4 = time4Avg.toFloat / num4;

      val total = num1 + num2 + num3 + num4

      val time1 = "incidentCount(hour 0 - 6): {" + num1 + "}" + "; averageIncidentWeight(hour 0 - 6): {" + average1 + "}" + "; theftCounts(0 - 6): {" + theftCount1 + "}"

      val time2 = "incidentCount(hour 6 - 12): {" + num2 + "}" + "; averageIncidentWeight(hour 6 - 12): {" + average2 + "}" + "; theftCounts(6 - 12): {" + theftCount2 + "}"

      val time3 = "incidentCount(hour 12 - 18): {" + num3 + "}" + "; averageIncidentWeight(hour 12 - 18): {" + average3 + "}" + "; theftCounts(12 - 18): {" + theftCount3 + "}"

      val time4 = "incidentCount(hour 18 - 24): {" + num4 + "}" + "; averageIncidentWeight(hour 18 - 24): {" + average4 + "}" + "; theftCounts(18 - 24): {" + theftCount4 + "}"
      val result = "totalIncidentCount: " + total + "----" + time1 + "----" + time2 + "----" + time3 + "----" + time4

      (value._1, (censusPortion, result))
    })

//    finalData.saveAsTextFile("output")
    finalData.saveAsTextFile(args(4))
  }
}

