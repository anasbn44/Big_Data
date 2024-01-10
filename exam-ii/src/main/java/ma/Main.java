package ma;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;

public class Main {
    public static void main(String[] args) {
        SparkSession ss = SparkSession.builder().appName("exam").getOrCreate();
        ss.sparkContext().setLogLevel("OFF");

        Dataset<Row> dataset = ss.read()
                .option("header", true)
                .option("multiline", true)
                .option("inferSchema", true)
                .csv("/bitnami/data.csv");

        dataset.show();

        dataset.printSchema();
        System.out.println("**********************************************************");
        System.out.println("1. Affichage du produit le plus vendu en terme de montant total.");
        dataset.groupBy("produit_id").agg(sum("montant").as("total_montant"))
                .orderBy(col("total_montant").desc())
                .limit(1)
                .show();

        System.out.println("**********************************************************");
        System.out.println("2. Affichage des 3 produits les plus vendus dans l'ensemble des données.");
        dataset.groupBy("produit_id").count()
                .orderBy(col("count").desc())
                .limit(3)
                .show();

        System.out.println("**********************************************************");
        System.out.println("3. Affichage du montant total des achats pour chaque produit.");
        dataset.groupBy("produit_id").agg(sum("montant").as("total_montant"))
                .show();


        ss.stop();
    }
}