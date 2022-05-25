
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DateType

import java.sql.{DriverManager, ResultSet}
import java.util.Properties

object MainAppEl {

    def read_from_postgre(spark: SparkSession, table: String, user: String, password: String) = {
        val jdbcDF = spark.read
            .format("jdbc")
            .option("url", "XXX")
            .option("dbtable", table)
            .option("user", user)
            .option("password", password)
            .option("driver", "org.postgresql.Driver")
            .load()
        jdbcDF

    }

    def main(args: Array[String]): Unit = {

        val db = "school_de"
        val AD = "rkhalikov"

        val user = args(0)
        val password = args(1)

        val spark = SparkSession
            .builder()
            .appName("Spark Hive Example")
            .enableHiveSupport()
            .getOrCreate()

        val outPath = f"hdfs: ... /hive/$db.db/seats_$AD"
        val DF_seats = read_from_postgre(spark, "demo.bookings.seats", user, password)
        DF_seats
            .write
            .mode("overwrite")
            .option("path", outPath)
            .saveAsTable(f"$db.seats_$AD")

        val outPath2 = f"hdfs: ... /hive/$db.db/flights_v_$AD"
        val DF_flights_v = read_from_postgre(spark, "demo.bookings.flights_v", user, password)
        DF_flights_v
            .withColumn("actual_departure_partitionBy", col("actual_departure").cast(DateType))
            .write
            .mode("overwrite")
            .partitionBy("actual_departure_partitionBy")
            .option("path", outPath2)  
            .saveAsTable(f"$db.flights_v_$AD")

    }
}
