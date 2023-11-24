package ma;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.year;

public class Main {
    public static void main(String[] args) {
        SparkSession ss = SparkSession.builder().
                appName("tp_5").
                master("local[*]")
                .getOrCreate();

        Dataset<Row> df = ss.read().option("header", true).csv("spark_tp/src/main/resources/incidents.csv");

        Dataset<Row> incidentsPerService = df.groupBy("service").count();
        incidentsPerService.show();

        Dataset<Row> dfWithYear = df.withColumn("year", year(col("date")));

        Dataset<Row> incidentsPerYear = dfWithYear.groupBy("year").count();

        incidentsPerYear = incidentsPerYear.orderBy(col("count").desc());

        incidentsPerYear.show(2);

    }
}