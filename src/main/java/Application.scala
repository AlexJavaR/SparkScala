import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.{SparkConf, SparkContext}

object Application {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val conf: SparkConf = new SparkConf().setAppName("Histogram").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)

    val sparkSession = SparkSession.builder.getOrCreate
    import sparkSession.implicits._

    val driversRDD: RDD[(String, String, String, String)] = sc.textFile("files/drivers.txt")
      .map(line => (line.split(", ")(0), line.split(", ")(1), line.split(", ")(2), line.split(", ")(3)))

    val taxiOrdersRDD: RDD[(String, String, Int, String)] = sc.textFile("files/taxi_orders.txt")
      .map(line => (line.split(", ")(0), line.split(", ")(1), line.split(", ")(2).toInt, line.split(", ")(3)))

    val driversDataSet = sparkSession.createDataset(driversRDD)
      .withColumnRenamed("_1", "id")
      .withColumnRenamed("_2", "name")
      .withColumnRenamed("_3", "address")
      .withColumnRenamed("_4", "email")

    val taxiOrdersDataSet = sparkSession.createDataset(taxiOrdersRDD)
      .withColumnRenamed("_1", "id")
      .withColumnRenamed("_2", "city")
      .withColumnRenamed("_3", "distance")
      .withColumnRenamed("_4", "date")

    println("Total trips: " + taxiOrdersDataSet.count)

    val countTripsToBostonLonger = taxiOrdersDataSet.filter(functions.lower(taxiOrdersDataSet.col("city")).equalTo("boston"))
      .filter("distance > 10")
      .count()

    println("Total trips in Boston longer than 10 km: " + countTripsToBostonLonger)

    val sumDistanceToBoston = taxiOrdersDataSet.filter(functions.lower(taxiOrdersDataSet.col("city")).equalTo("boston"))
      .groupBy()
      .sum("distance")
      .first()
      .get(0)

    println("Sum of all distances trips in Boston: " + sumDistanceToBoston)

    val joinDriversTaxiOrders = driversDataSet.join(taxiOrdersDataSet, "id")

    joinDriversTaxiOrders
      .groupBy("name")
      .sum("distance")
      .sort(functions.desc("sum(distance)"))
      .show(3)
  }

}
