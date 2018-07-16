object ContingencyExample {

  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.types._
  import org.apache.spark.sql.SparkSession
  import org.apache.spark.{SparkConf, SparkContext}
  import com.manualrg.mlutils._

  val spark = SparkSession
    .builder()
    .appName("ContingencyExample")
    .getOrCreate()
  val sc = spark.sparkContext
  import spark.implicits._

  case class Record(id :Int, x1: String, x2: String, x3 :String, x4 :Double, label :Double)

  def main(args: Array[String]): Unit = {


    val df = spark.createDataFrame(Seq(Record(0, "a", "z", "m", 10.0, 0.0),
      Record(1, "b", "x", "m", 20.0, 0.0),
      Record(2, "a", "y", "l", 30.0, 0.0),
      Record(3, "a", "y", "n", 40.0, 1.0),
      Record(4, "b", "x", "n", 50.0, 1.0),
      Record(5, "c", "z", "m", 60.0, 1.0)
    ))

    println("Display input data")
    df.show()

    val myContingencyTable = ContingencyTable
    val inputCols = Array("x1", "x2", "x3")
    val labelCol = "label"
    val contingencyReport = myContingencyTable.fit(df, inputCols, labelCol)

    println("Display contingency table")
    contingencyReport.show()
  }
}
