import org.apache.commons.net.ntp.TimeStamp
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.sql.catalyst.expressions.Substring
import org.apache.spark.sql.functions.{abs, avg, col, concat_ws, count, countDistinct, current_timestamp, date_format, desc, expr, from_unixtime, lit, lpad, mean, min, rank, round, second, sum, to_date, to_timestamp, to_utc_timestamp}
import org.apache.spark.sql.types.{DateType, IntegerType, LongType, TimestampType}
import org.apache.spark.sql.expressions.Window

import java.sql.{DriverManager, ResultSet}
import java.util.Properties

object MainAppCalc {
    val db = "school_de"

// ###############################################################################################
//    -- 1. Вывести максимальное количество человек в одном бронировании


    def task_01(spark: SparkSession) = {
        val RESULTS = f"$db.results_rkhalikov"
        val colsResults = "response"
        val table_name = "bookings_tickets"
        val tickets = spark.table(f"school_de.$table_name")

        val answer = (
            tickets
                .groupBy("book_ref")
                .agg(count("ticket_no").as("max_counts"))
                .orderBy(col("max_counts").desc)
                .select("max_counts")
                .limit(1)
            )
        val columns = Seq("id", colsResults)
        val data = Seq((1, answer.collect()(0)(0).toString))
        val df = spark.createDataFrame(data).toDF(columns:_*)

        df
            .write
            .mode("overwrite")
            .saveAsTable(RESULTS)

    }

    // ###############################################################################################
//    -- 2. Вывести количество бронирований с количеством людей
//    -- больше среднего значения людей на одно бронирование

    def task_02(spark: SparkSession) = {
        val RESULTS = f"$db.results_rkhalikov"
        val colsResults = "response"
        val table_name = "bookings_tickets"
        val tickets = spark.table(f"school_de.$table_name")

        val t1 = (
            tickets
                .groupBy("book_ref")
                .agg(count("ticket_no").as("count_tickets"))
                .orderBy(col("count_tickets").desc)
            )

        val temp = (
            t1
                .agg(avg("count_tickets").alias("avg_count_tickets"))
                .select("avg_count_tickets")
        ).collect()(0)(0)

        val t2 = (
            tickets
                .groupBy("book_ref")
                .agg(count("ticket_no").alias("count"))
                .filter(col("count") > temp)
        )

        val answer = (
            t2
                .count()
        )

        val columns = Seq("id", colsResults)
        val data = Seq((2, answer))
        val df = spark.createDataFrame(data).toDF(columns:_*)

        df
            .write
            .mode("append")
            .saveAsTable(RESULTS)
    }

    // ###############################################################################################
//    -- 3. Вывести количество бронирований,
//    -- у которых состав пассажиров повторялся два и более раза,
//    -- среди бронирований с максимальным количеством людей (п.1)?

    def task_03(spark: SparkSession) = {
        val RESULTS = f"$db.results_rkhalikov"
        val colsResults = "response"
        val table_name = "bookings_tickets"
        val tickets = spark.table(f"school_de.$table_name")

        val temp = (
            tickets
                .groupBy("book_ref")
                .agg(count("*").as("count_tickets"))
                .orderBy(col("count_tickets").desc)
                .limit(1)
            ).collect()(0)(0)

        val t1 = (
            tickets
                .groupBy("book_ref")
                .agg(count("ticket_no").alias("count_tickets"))
                .filter(col("count_tickets") === temp)
                .select("book_ref")
                .withColumn("book_refff", col("book_ref"))
                .select("book_refff")
            )

        val t2 = tickets.join(t1, tickets("book_ref") === t1("book_refff"))

        val t3 = (
            t2
                .groupBy("book_ref", "passenger_name")
                .agg(count("*").alias("count_this"))
                .filter(col("count_this") > 1)
                .orderBy(col("book_ref").desc, col("passenger_name").desc)
                .select(col("book_ref"), col("passenger_name"), col("count_this"))
        )
        val answer = t3.count()

        val columns = Seq("id", colsResults)
        val data = Seq((3, answer))
        val df = spark.createDataFrame(data).toDF(columns:_*)

        df
            .write
            .mode("append")
            .saveAsTable(RESULTS)
        0
    }


    // ###############################################################################################

    //    -- 4. Вывести номера брони и контактную информацию по пассажирам
    //    -- в брони (passenger_id, passenger_name, contact_data)
    //    -- с количеством людей в брони = 3
    def task_04(spark: SparkSession) = {

        val RESULTS = f"$db.results_rkhalikov"
        val colsResults = "response"
        val table_name = "bookings_tickets"
        val tickets = spark.table(f"school_de.$table_name")

        val t1 = (
            tickets
                .groupBy("book_ref")
                .agg(count("*").alias("count"))
                .filter(col("count") === 3)
                .withColumn("book_ref_new", col("book_ref"))
        )
        val t2 = tickets.join(t1, t1("book_ref_new") === tickets("book_ref"), "inner")

        val answer = (
            t2
                .withColumn("id", lit(4))
                .withColumn(
                    "response",
                    concat_ws(
                        "|",
                        col("book_ref_new"),
                        col("passenger_id"),
                        col("passenger_name"),
                        col("contact_data")
                    )
                )
                .orderBy(col("response"))
                .select(col("id"), col("response"))
        )

        answer
            .write
            .mode("append")
            .saveAsTable(RESULTS)
}
    // ###############################################################################################

//    -- 5. Вывести максимальное количество перелётов на бронь
    def task_05(spark: SparkSession) = {

        val RESULTS = f"$db.results_rkhalikov"
        val colsResults = "response"
        val table_name_tickets = "bookings_tickets"
        val tickets = spark.table(f"school_de.$table_name_tickets")

        val table_name_ticket_flights = "bookings_ticket_flights"
        val ticket_flights = spark.table(f"school_de.$table_name_ticket_flights")

        val t1 = tickets.join(ticket_flights, tickets("ticket_no") === ticket_flights("ticket_no"), "inner")

        val answer = (
            t1
                .groupBy(col("book_ref"))
                .agg(count("*").alias("cnt"))
                .orderBy(col("cnt").desc)
                .select(col("cnt"))
                .limit(1)
        ).collect()(0)(0)

        val columns = Seq("id", colsResults)
        val data = Seq((5, answer.toString))
        val df = spark.createDataFrame(data).toDF(columns:_*)

        df
            .write
            .mode("append")
            .saveAsTable(RESULTS)
    }
    // ###############################################################################################

//    -- 6. Вывести максимальное количество перелётов на пассажира в одной брони

    def task_06(spark: SparkSession) = {

        val RESULTS = f"$db.results_rkhalikov"
        val colsResults = "response"

        val table_name_tickets = "bookings_tickets"
        val tickets = spark.table(f"school_de.$table_name_tickets")

        val table_name_ticket_flights = "bookings_ticket_flights"
        val ticket_flights = spark.table(f"school_de.$table_name_ticket_flights")

        val table_name_bookings_flights_v = "bookings_flights_v"
        val flights_v = spark.table(f"school_de.$table_name_bookings_flights_v")

        val t1 = (
            tickets
                .join(
                    ticket_flights,
                    tickets("ticket_no") === ticket_flights("ticket_no"), "inner"
                )
            )
            .join(
                flights_v,
                ticket_flights("flight_id") === flights_v("flight_id")
            )

        val answer = (
            t1
                .groupBy(col("bookings_tickets.book_ref"), col("bookings_ticket_flights.ticket_no"))
                .agg(count("*").alias("cnt"))
                .orderBy(col("cnt").desc)
                .select(col("cnt"))
                .limit(1)
            ).collect()(0)(0)

        val columns = Seq("id", colsResults)
        val data = Seq((6, answer.toString))
        val df = spark.createDataFrame(data).toDF(columns:_*)

        df
            .write
            .mode("append")
            .saveAsTable(RESULTS)
    }
    // ###############################################################################################

//    -- 7. Вывести максимальное количество перелётов на пассажира

    def task_07(spark: SparkSession) = {

        val RESULTS = f"$db.results_rkhalikov"
        val colsResults = "response"

        val table_name_tickets = "bookings_tickets"
        val tickets = spark.table(f"school_de.$table_name_tickets")

        val table_name_ticket_flights = "bookings_ticket_flights"
        val ticket_flights = spark.table(f"school_de.$table_name_ticket_flights")

        val table_name_bookings_flights_v = "bookings_flights_v"
        val flights_v = spark.table(f"school_de.$table_name_bookings_flights_v")

        val t1 = (
            tickets
                .join(
                    ticket_flights,
                    tickets("ticket_no") === ticket_flights("ticket_no"), "inner"
                )
            )
            .join(
                flights_v,
                ticket_flights("flight_id") === flights_v("flight_id")
            )

        val answer = (
            t1
                .groupBy(col("bookings_tickets.passenger_id"))
                .agg(
                    count(
                        "bookings_ticket_flights.flight_id").alias("cnt"
                    )
                )
                .orderBy(col("cnt").desc)
                .select(col("cnt"))
                .limit(1)
            ).collect()(0)(0)

        val columns = Seq("id", colsResults)
        val data = Seq((7, answer.toString))
        val df = spark.createDataFrame(data).toDF(columns:_*)

        df
            .write
            .mode("append")
            .saveAsTable(RESULTS)
    }
    // ###############################################################################################

//    -- 8. Вывести контактную информацию по пассажиру(ам)
//    -- (passenger_id, passenger_name, contact_data)
//    -- и общие траты на билеты, для пассажира потратившему
//    -- минимальное количество денег на перелеты
    def task_08(spark: SparkSession) = {

        val RESULTS = f"$db.results_rkhalikov"
        val colsResults = "response"

        val table_name_tickets = "bookings_tickets"
        val tickets = spark.table(f"school_de.$table_name_tickets")

        val table_name_ticket_flights = "bookings_ticket_flights"
        val ticket_flights = spark.table(f"school_de.$table_name_ticket_flights")

        val table_name_bookings_flights_v = "bookings_flights"
        val flights = spark.table(f"school_de.$table_name_bookings_flights_v")

        val t1 = (
            tickets
                .join(
                    ticket_flights,
                    tickets("ticket_no") === ticket_flights("ticket_no"), "inner"
                )
            )
            .join(
                flights,
                ticket_flights("flight_id") === flights("flight_id")
            )

        val temp = (
            t1
                .filter(col("bookings_flights.status") !== "Cancelled")
                .groupBy(
                    col("passenger_id"),
                    col("passenger_name"),
                    col("contact_data")
                )
                .agg(
                    sum(
                        col("bookings_ticket_flights.amount")
                    ).alias("money_spend")
                )
                .orderBy(col("money_spend"))
                .select("money_spend")
                .limit(1)

        ).collect()(0)(0)

        val t2 = (
            t1
                .filter(col("status") !== "Cancelled")
                .groupBy(
                    col("passenger_id"),
                    col("passenger_name"),
                    col("contact_data")
                )
                .agg(
                    sum(
                        col("amount")
                    ).alias("sum_amount")
                )
                .filter(col("sum_amount") === temp)
                .orderBy(
                    col("passenger_id"),
                    col("passenger_name"),
                    col("contact_data"),
                    col("sum_amount")
                )
        )

        val answer = (
            t2
                .withColumn("id", lit(8))
                .withColumn(
                    "response",
                    concat_ws(
                        "|",
                        col("passenger_id"),
                        col("passenger_name"),
                        col("contact_data"),
                        col("sum_amount")
                    )
                )
                .orderBy(col("response"))
                .select(
                    col("id"),
                    col("response")
                )
            )

        answer
            .write
            .mode("append")
            .saveAsTable(RESULTS)
    }
    // ###############################################################################################

//    -- 9. Вывести контактную информацию по пассажиру(ам)
//    -- (passenger_id, passenger_name, contact_data) и общее время в полётах,
//    -- для пассажира, который провёл максимальное время в полётах


    def task_09(spark: SparkSession) = {
        val RESULTS = f"$db.results_rkhalikov"
        val colsResults = "response"

        val table_name_tickets = "bookings_tickets"
        val tickets = spark.table(f"school_de.$table_name_tickets")

        val table_name_ticket_flights = "bookings_ticket_flights"
        val ticket_flights = spark.table(f"school_de.$table_name_ticket_flights")

        val table_name_bookings_flights_v = "bookings_flights_v"
        val flights_v = spark.table(f"school_de.$table_name_bookings_flights_v")

        val join_table = (
            tickets
                .join(ticket_flights, tickets("ticket_no") === ticket_flights("ticket_no"))
                .join(flights_v, ticket_flights("flight_id") === flights_v("flight_id"))
            )

//        ticket_flights.cache()
//        flights_v.cache()
//        tickets.cache()
//        join_table.cache()

        val t1 = (
            join_table
                .filter(col("bookings_flights_v.status") === "Arrived")
                .withColumn(
                    "time_to_fly",
                    col("bookings_flights_v.actual_arrival").cast(LongType) - col("bookings_flights_v.actual_departure").cast(LongType)
                )
                .select(
                    "bookings_tickets.ticket_no",
                    "bookings_tickets.passenger_id",
                    "bookings_tickets.passenger_name",
                    "bookings_tickets.contact_data",
                    "bookings_ticket_flights.flight_id",
                    "bookings_flights_v.actual_departure",
                    "bookings_flights_v.actual_arrival",
                    "time_to_fly"
                )
        )

        val t2 = (
            t1
                .groupBy(
                    col("passenger_id"),
                    col("passenger_name"),
                    col("contact_data")
                )
                .agg(
                    sum("time_to_fly").as("sum_time_to_fly")
                )
                .orderBy(col("sum_time_to_fly").desc)
                .select(
                    col("passenger_id"),
                    col("passenger_name"),
                    col("contact_data"),
                    col("sum_time_to_fly")
                )
        )

        val windowSpec = Window.orderBy(col("sum_time_to_fly").desc)

        val t3 = (
            t2
                .withColumn("rnk", rank().over(windowSpec))
        )

        val answer = (
            t3
                .filter(col("rnk") === 1)
                .withColumn(
                    "hh",
                    (col("sum_time_to_fly") / 3600).cast(IntegerType)
                )
                .withColumn(
                    "mm",
                    ((col("sum_time_to_fly") - col("hh") * 3600) / 60).cast(IntegerType)
                )
                .withColumn(
                    "ss",
                    lpad((col("sum_time_to_fly") - col("hh") * 3600 - col("mm") * 60).cast(IntegerType), 2, "0")
                )
                .withColumn(
                    "time_h_m_s",
                    concat_ws(
                        ":",
                        col("hh"),
                        col("mm"),
                        col("ss")
                    )
                )
                .withColumn(
                    "response",
                    concat_ws(
                        "|",
                        col("passenger_id"),
                        col("passenger_name"),
                        col("contact_data"),
                        col("time_h_m_s")
                    )
                )
                .withColumn("id", lit(9))
                .select(col("id"), col("response"))
                .orderBy(col("response"))
        )

        answer
            .write
            .mode("append")
            .saveAsTable(RESULTS)
}


    //    -- 10. Вывести город(а) с количеством аэропортов больше одного

    def task_10(spark: SparkSession) = {

        val RESULTS = f"$db.results_rkhalikov"
        val colsResults = "response"

        val table_name_airports = "bookings_airports"
        val airports = spark.table(f"school_de.$table_name_airports")

        val t1 = (
            airports
                .groupBy(
                    col("city")
                )
                .agg(
                    count("*")
                        .alias("cnt")
                )
                .filter(
                    col("cnt") > 1
                )
                .orderBy("city")
                .select(
                    col("city")
                )
        )

        val answer = (
            t1
                .withColumn("id", lit(10))
                .withColumn(
                    "response",
                    col("city")
                )
                .drop(col("city"))
        )

        answer
            .write
            .mode("append")
            .saveAsTable(RESULTS)
    }


//    -- 11. Вывести город(а), у которого самое меньшее количество городов прямого сообщения

    def task_11(spark: SparkSession) = {

        val RESULTS = f"$db.results_rkhalikov"
        val colsResults = "response"

        val table_name_routes = "bookings_routes"
        val routes = spark.table(f"school_de.$table_name_routes")

        val t1 = (
            routes
                .groupBy(
                    col("departure_city")
                )
                .agg(
                    countDistinct("arrival_city")
                        .alias("route")
                )
                .orderBy(col("route"))
                .select(
                    col("departure_city"),
                    col("route")
                )
        )

        val temp = (
            t1
                .agg(
                    min(col("route"))
                        .alias("min_route")
                )
                .select("min_route")
                .limit(1)
            ).collect()(0)(0)

        val t2 = (
            t1
                .filter(
                    col("route") === temp
                )
                .orderBy(col("departure_city"))
                .select(col("departure_city"))
        )

        val answer = (
            t2
                .withColumn(
                    "id",
                    lit(11)
                )
                .withColumn(
                    "response",
                    col("departure_city")
                )
                .drop(col("departure_city"))
        )

        answer
            .write
            .mode("append")
            .saveAsTable(RESULTS)
    }
    // ###############################################################################################

//    -- 12. Вывести пары городов, у которых нет прямых сообщений исключив реверсные дубликаты

    def task_12(spark: SparkSession) = {

        val RESULTS = f"$db.results_rkhalikov"
        val colsResults = "response"

        val table_name_airports = "bookings_airports"
        val airports = spark.table(f"school_de.$table_name_airports")

        val table_name_flights_v = "bookings_flights_v"
        val flights_v = spark.table(f"school_de.$table_name_flights_v")

        val t1 = (
            airports
                .select("city")
                .withColumn("city_t1", col("city"))
                .drop(col("city"))
                .distinct()
        )

        val t2 = (
            airports
                .join(
                    t1,
                    airports("city") < t1("city_t1"),
                    "cross"
                )
            )
            .select(
                col("city").alias("c1"),
                col("city_t1").alias("c2")
            )

        val t3 = (
            flights_v
                .select(
                    col("departure_city").alias("c1"),
                    col("arrival_city").alias("c2")
                )
        )

        val t4 = (
            t2
                .except(t3)
        )

        val answer = (
            t4
                .withColumn("id", lit(12))
                .withColumn(
                    "response",
                    concat_ws(
                        "|",
                        col("c1"),
                        col("c2")
                    )
                )
                .drop(
                    col("c1")
                )
                .drop(
                    col("c2")
                )
                .orderBy(col("response"))
        )

        answer
            .write
            .mode("append")
            .saveAsTable(RESULTS)


    }

//    -- 13. Вывести города, до которых нельзя добраться без пересадок из Москвы?
    def task_13(spark: SparkSession) = {

        val RESULTS = f"$db.results_rkhalikov"
        val colsResults = "response"

        val table_name_airports = "bookings_airports"
        val airports = spark.table(f"school_de.$table_name_airports")

        val table_name_flights_v = "bookings_flights_v"
        val flights_v = spark.table(f"school_de.$table_name_flights_v")

        val t1 = (
            airports
                .select("city")
                .withColumn("city_t1", col("city"))
                .drop(col("city"))
                .distinct()
            )

        val t2 = (
            airports
                .join(
                    t1,
                    airports("city") =!= t1("city_t1") and airports("city") === "Москва",
                    "cross"
                )
            )
            .select(
                col("city").alias("c1"),
                col("city_t1").alias("c2")
            )

        val t3 = (
            flights_v
                .filter(
                    col("departure_city") === "Москва"
                )
                .select(
                    col("departure_city").alias("c1"),
                    col("arrival_city").alias("c2")
                )
            )

        val t4 = (
            t2
                .except(t3)
            )

        val answer = (
            t4
                .withColumn("id", lit(13))
                .withColumn(
                    "response",
                    col("c2")
                )
                .drop(
                    col("c1")
                )
                .drop(
                    col("c2")
                )
                .orderBy(col("response"))
            )

        answer
            .write
            .mode("append")
            .saveAsTable(RESULTS)

    }

//    -- 14. Вывести модель самолета, который выполнил больше всего рейсов

    def task_14(spark: SparkSession) = {

        val RESULTS = f"$db.results_rkhalikov"
        val colsResults = "response"

        val talbe_name_aircrafts = "bookings_aircrafts"
        val aircrafts = spark.table(f"school_de.$talbe_name_aircrafts")

        val table_name_flights_v = "bookings_flights_v"
        val flights_v = spark.table(f"school_de.$table_name_flights_v")

        val t1 = (
            flights_v
                .filter(col("status") === "Arrived")
                .groupBy(col("aircraft_code"))
                .agg(count("*").as("cnt"))
                .orderBy(col("cnt").desc)
                .select(
                    col("aircraft_code"),
                    col("cnt")
                )
        )

        val windowSpec = Window.orderBy(col("cnt").desc)

        val t2 = (
            t1
                .withColumn("rnk", rank().over(windowSpec))
        )

        val t3 = (
            t2
                .filter(col("rnk") === 1)
        )

        val t4 = aircrafts.join(t3, aircrafts("aircraft_code") === t3("aircraft_code"))

        val answer = (
            t4
                .withColumn("id", lit(14))
                .withColumn("response", col("model"))
                .select(
                    col("id"),
                    col("response")
                )
        )

        answer
            .write
            .mode("append")
            .saveAsTable(RESULTS)

    }

//    -- 15. Вывести модель самолета, который перевез больше всего пассажиров
    def task_15(spark: SparkSession) = {

        val RESULTS = f"$db.results_rkhalikov"
        val colsResults = "response"

        val talbe_name_aircrafts = "bookings_aircrafts"
        val aircrafts = spark.table(f"school_de.$talbe_name_aircrafts")

        val table_name_flights_v = "bookings_flights_v"
        val flights_v = spark.table(f"school_de.$table_name_flights_v")

        val table_name_ticket_flights = "bookings_ticket_flights"
        val ticket_flights = spark.table(f"school_de.$table_name_ticket_flights")

        val join_table = (
            ticket_flights
                .join(
                    flights_v,
                    ticket_flights("flight_id") === flights_v("flight_id")
                )
        )

        val t1 = (
            join_table
                .filter(col("status") === "Arrived")
                .groupBy(col("aircraft_code"))
                .agg(count("*").as("cnt"))
                .select(
                    col("aircraft_code"),
                    col("cnt")
                )
            )

        val windowSpec = Window.orderBy(col("cnt").desc)

        val t2 = (
            t1
                .withColumn("rnk", rank().over(windowSpec))
            )

        val t3 = (
            t2
                .filter(col("rnk") === 1)
            )

        val t4 = aircrafts.join(t3, aircrafts("aircraft_code") === t3("aircraft_code"))

        val answer = (
            t4
                .withColumn("id", lit(15))
                .withColumn("response", col("model"))
                .select(
                    col("id"),
                    col("response")
                )
            )

        answer
            .write
            .mode("append")
            .saveAsTable(RESULTS)
    }

//    -- 16. Вывести отклонение в минутах
//    -- суммы запланированного времени перелета от фактического по всем перелётам
    def task_16(spark: SparkSession) = {

    val RESULTS = f"$db.results_rkhalikov"
    val colsResults = "response"

    val table_name_flights_v = "bookings_flights_v"
    val flights_v = spark.table(f"school_de.$table_name_flights_v")

    val t1 = (
        flights_v
            .filter(col("status") === "Arrived")
            .withColumn(
                "plan_time",
                (col("scheduled_arrival").cast(LongType) - col("scheduled_departure").cast(LongType)) / 60
            )
            .withColumn(
                "actual_time",
                (col("actual_arrival").cast(LongType) - col("actual_departure").cast(LongType)) / 60
            )
            .select(col("plan_time"), col("actual_time"))
    )
    val temp = (
        t1
            .withColumn("diff_time", col("plan_time") - col("actual_time"))
            .agg(
                abs(
                    sum(
                        col("diff_time")
                    )
                ).as("sum_diff")
            )
            .select(col("sum_diff"))
    )

    val answer = (
        temp
            .withColumn("id", lit(16))
            .withColumn("response", col("sum_diff").cast(IntegerType))
            .select(
                col("id"),
                col("response")
            )
        )

    answer
        .write
        .mode("append")
        .saveAsTable(RESULTS)
}

//    -- 17. Вывести города, в которые осуществлялся перелёт из Санкт-Петербурга 2016-09-13
    def task_17(spark: SparkSession) = {
        val RESULTS = f"$db.results_rkhalikov"
        val colsResults = "response"

        val table_name_flights_v = "bookings_flights_v"
        val flights_v = spark.table(f"school_de.$table_name_flights_v")

        val t1 = (
            flights_v
                .select(
                    col("status"),
                    col("departure_city"),
                    col("arrival_city"),
                    date_format(flights_v("actual_departure"), "yyyy-MM-dd").alias("new_date")
                )
                .filter(
                    col("status") === "Arrived" or col("status") === "Departed"
                )
                .filter(col("departure_city") === "Санкт-Петербург")
                .filter(col("new_date") === "2016-09-13")
                .select(col("arrival_city"))
            ).distinct()

        val answer = (
            t1
                .withColumn("id", lit(17))
                .withColumn("response", col("arrival_city"))
                .select(
                    col("id"),
                    col("response")
                )
                .orderBy(col("response"))
            )

        answer
            .write
            .mode("append")
            .saveAsTable(RESULTS)
}

    //    -- 18. Вывести перелёт(ы) с максимальной стоимостью всех билетов
    def task_18(spark: SparkSession) = {
        val RESULTS = f"$db.results_rkhalikov"
        val colsResults = "response"

        val table_name_flights_v = "bookings_flights_v"
        val flights_v = spark.table(f"school_de.$table_name_flights_v")

        val table_name_ticket_flights = "bookings_ticket_flights"
        val ticket_flights = spark.table(f"school_de.$table_name_ticket_flights")

        val join_table = (
            ticket_flights
                .join(
                    flights_v,
                    ticket_flights("flight_id") === flights_v("flight_id")
                )
            )

        val t1 = (
            join_table
                .filter(col("status") =!= "Cancelled")
                .groupBy(col("bookings_flights_v.flight_id"))
                .agg(sum(col("amount")).as("sum_amount"))
                .orderBy(col("sum_amount").desc)
                .select(
                    col("flight_id"),
                    col("sum_amount")
                )
            )

        val windowSpec = Window.orderBy(col("sum_amount").desc)

        val t2 = (
            t1
                .withColumn("rnk", rank().over(windowSpec))
            )

        val t3 = (
            t2
                .filter(col("rnk") === 1)

            )

        val answer = (
            t3
                .withColumn("id", lit(18))
                .withColumn("response", col("flight_id"))
                .select(
                    col("id"),
                    col("response")
                )
                .orderBy(col("response"))
            )

        answer
            .write
            .mode("append")
            .saveAsTable(RESULTS)
    }

//    -- 19. Выбрать дни в которых было осуществлено минимальное количество перелётов
    def task_19(spark: SparkSession) = {
        val RESULTS = f"$db.results_rkhalikov"
        val colsResults = "response"

        val table_name_flights_v = "bookings_flights_v"
        val flights_v = spark.table(f"school_de.$table_name_flights_v")

        val t1 = (
            flights_v
                .filter(
                    col("status") =!= "Cancelled" && col("actual_departure").isNotNull
                )
                .withColumn("ac_dat", to_utc_timestamp(col("actual_departure"), "Europe/Moscow"))
                .withColumn("gl_ac_dat", col("ac_dat").cast(DateType))
                .groupBy(col("gl_ac_dat"))
                .agg(count("flight_id").as("cnt"))
                .orderBy(col("cnt").desc)
                .select(
                    col("gl_ac_dat"),
                    col("cnt")
                )
            )

        val windowSpec = Window.orderBy(col("cnt"))

        val t2 = (
            t1
                .withColumn("rnk", rank().over(windowSpec))
                .filter(col("rnk") === 1)

            )

        val answer = (
            t2
                .withColumn("id", lit(19))
                .withColumn("response", col("gl_ac_dat"))
                .select(
                    col("id"),
                    col("response")
                )
                .orderBy(col("response"))
            )

        answer
            .write
            .mode("append")
            .saveAsTable(RESULTS)
    }

//    -- 20. Вывести среднее количество вылетов в день из Москвы за 09 месяц 2016 года
    def task_20(spark: SparkSession) = {
        val RESULTS = f"$db.results_rkhalikov"
        val colsResults = "response"

        val table_name_flights_v = "bookings_flights_v"
        val flights_v = spark.table(f"school_de.$table_name_flights_v")

        val t1 = (
            flights_v
                .select(col("status"), col("departure_city"), date_format(flights_v("actual_departure"), "yyyy-MM-dd").alias("new_date"))
                .filter(col("departure_city") === "Москва")
                .filter(col("status") === "Departed" or col("status") === "Arrived")
                .filter(col("new_date").between("2016-09-01", "2016-09-30"))
                .count()
            )

        val answer = t1 / 30

        val columns = Seq("id", colsResults)
        val data = Seq((20, answer))
        val df = spark.createDataFrame(data).toDF(columns:_*)

        df
            .write
            .mode("append")
            .saveAsTable(RESULTS)
    }

//    -- 21. Вывести топ 5 городов у которых среднее время перелета до пункта назначения больше 3 часов
    def task_21(spark: SparkSession) = {
        val RESULTS = f"$db.results_rkhalikov"
        val colsResults = "response"

        val table_name_flights_v = "bookings_flights_v"
        val flights_v = spark.table(f"school_de.$table_name_flights_v")


        val t1 = (
            flights_v
                .filter(col("status") === "Arrived")
                .withColumn("start_date", lit("00:00:00"))
                .withColumn("start_date_timestamp", to_timestamp(col("start_date"), "HH:mm:ss"))
                .withColumn("actual_duration_timestamp", to_timestamp(col("actual_duration"), "HH:mm:ss"))
                .withColumn(
                    "diff_sec",
                    col("actual_duration_timestamp").cast(LongType) - col("start_date_timestamp").cast(LongType)
                )
                .select(
                    "diff_sec",
                    "actual_duration",
                    "departure_city"
                )
                .groupBy("departure_city")
                .agg(mean("diff_sec").as("avg_sec"))
                .filter(col("avg_sec") > 10800)
                .orderBy(col("avg_sec").desc)
                .limit(5)
            )

        val answer = (
            t1
                .withColumn("id", lit(21))
                .withColumn("response", col("departure_city"))
                .select(
                    col("id"),
                    col("response")
                )
                .orderBy(col("response"))
            )


        answer
            .write
            .mode("append")
            .saveAsTable(RESULTS)
    }

    def main(args: Array[String]): Unit = {

        val warehouseLocation = "hdfs:// ... external/hive/school_de.db"
        val spark = SparkSession
            .builder()
            .master("yarn")
            .appName("MainAppCalc_Omega515")
            .config("spark.sql.warehouse.dir", warehouseLocation)
            .config("spark.driver.extraJavaOptions", "-Duser.timezone=GMT")
            .config("spark.executor.extraJavaOptions", "-Duser.timezone=GMT")
            .config("spark.sql.session.timeZone", "UTC")
            .enableHiveSupport()
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        task_01(spark)

        task_02(spark)

        task_03(spark)

        task_04(spark)

        task_05(spark)

        task_06(spark)

        task_07(spark)

        task_08(spark)

        task_09(spark)

        task_10(spark)

        task_11(spark)

        task_12(spark)

        task_13(spark)

        task_14(spark)

        task_15(spark)

        task_16(spark)

        task_17(spark)

        task_18(spark)

        task_19(spark)

        task_20(spark)

        task_21(spark)

        spark.close()
    }
}

