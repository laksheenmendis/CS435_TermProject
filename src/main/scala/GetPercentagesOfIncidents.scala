
import org.apache.spark._


object GetPercentagesOfIncidents {


  def countIncidents(incidentArray: Array[String]) : String = {
    var numIncidents = 0;
    var numVehicleIncidents = 0
    var numTheftIncidents = 0
    var numDrugIncidents = 0
    var numWeaponIncidents = 0
    var numSexualIncidents = 0
    var numViolentIncidents = 0
    var numOffenseIncidents = 0
    var numReportIncidents = 0
    var numOtherIncidents = 0

    var index = 0
    while(index != incidentArray.length) {
      val array = incidentArray(index).split(";")

      if(array(2).contains("Vehicle") || array(2).contains("Traffic")) {
        numVehicleIncidents = numVehicleIncidents + 1
      }
      else if(array(2).contains("Theft") || array(2).contains("Stolen") || array(2).contains("Robbery") || array(2).contains("Burglary") || array(2).contains("Embezzlement")) {
        numTheftIncidents = numTheftIncidents + 1
      }
      else if(array(2).contains("Drug") || array(2).contains("Liquor")) {
        numDrugIncidents = numDrugIncidents + 1
      }
      else if(array(2).contains("Weapon")) {
        numWeaponIncidents = numWeaponIncidents + 1
      }
      else if(array(2).contains("Rape") || array(2).contains("Sex") || array(2).contains("Trafficking") || array(2).contains("Prostitution")) {
        numSexualIncidents = numSexualIncidents + 1
      }
      else if(array(2).contains("Assault") || array(2).contains("Suicide") || array(2).contains("Homicide") || array(2).contains("Malicious")) {
        numViolentIncidents = numViolentIncidents + 1
      }
      else if(array(2).contains("Offense") || array(2).contains("Offence")) {
        numOffenseIncidents = numOffenseIncidents + 1
      }
      else if(array(2).contains("Report")) {
        numReportIncidents = numReportIncidents + 1
      }
      else {
        numOtherIncidents = numOtherIncidents + 1
      }
      numIncidents = numIncidents + 1
      index = index + 1
    }

    val result = "Total Incidents: " + numIncidents + "; Vehicle Incidents: " + numVehicleIncidents + "; Theft Incidents: " + numTheftIncidents + "; Drug/Liquor Incidents: " + numDrugIncidents +
      "; Incidents Involving Weapons: " + numWeaponIncidents + "; Sexual Incidents: " + numSexualIncidents + "; Violent Incidents/Suicide: " + numViolentIncidents +
      "; Incidents Involving Offenses: " + numOffenseIncidents + "; Reporting Incidents: " + numReportIncidents + "; Other Incidents: " + numOtherIncidents

    (result)
  }
  def main(args: Array[String]) : Unit = {
//    val conf = new SparkConf().setAppName("censusAnalysis").setMaster("local")
    val conf = new SparkConf().setAppName("censusAnalysis").setMaster("yarn")
    val sc = new SparkContext(conf)

    //    val incidentFile1 = sc.textFile("data/testingData/incidents1")
    val incidentFile1 = sc.textFile(args(0))
    //    val incidentFile2 = sc.textFile("data/testingData/incidents2.txt")
    val incidentFile2 = sc.textFile(args(1))


    var incidentData1 = incidentFile1.map(value => {
      val result = value.substring(1,value.length-1)
      val data = result.split(",")

      //zip code -- zip code, time, incident category, police district
      ("SanFrancisco", data(0)+";"+data(3)+";"+data(15)+";"+data(21))
    })
    var incidentData2 = incidentFile2.map(value => {
      val result = value.substring(1,value.length-1)
      val data = result.split(",")

      //zip code -- zip code, time, incident category, police district
      ("SanFrancisco", data(0)+";"+data(3)+";"+data(15)+";"+data(21))
    })


    //merging the incident data
    var incidents = incidentData1.union(incidentData2)

    var joinedData = incidents.reduceByKey((value1,value2) => {
      (value1 + "----" + value2)
    })

    //Getting global incident data
    val wholeIncidentData = joinedData.map(value => {
      val incidentArray = value._2.split("----")

      val result = countIncidents(incidentArray)

      (value._1, result)
    })

    wholeIncidentData.saveAsTextFile(args(2))




    incidentData1 = incidentFile1.map(value => {
      val result = value.substring(1,value.length-1)
      val data = result.split(",")

      //zip code -- zip code, time, incident category, police district
      (data(21), data(0)+";"+data(3)+";"+data(15))
    })
    incidentData2 = incidentFile2.map(value => {
      val result = value.substring(1,value.length-1)
      val data = result.split(",")

      //zip code -- zip code, time, incident category, police district
      (data(21), data(0)+";"+data(3)+";"+data(15))
    })

    incidents = incidentData1.union(incidentData2)

    joinedData = incidents.reduceByKey((value1,value2) => {
      (value1 + "----" + value2)
    })

    val policeDistrictData = joinedData.map(value => {
      val incidentArray = value._2.split("----")

      val result = countIncidents(incidentArray)

      (value._1, result)
    })

    policeDistrictData.saveAsTextFile(args(3))

  }
}
