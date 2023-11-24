package ma;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

public class ExeSnd {
    public static void main(String[] args) {
        SparkSession ss = SparkSession.builder().
                appName("tp_5_2").
                master("local[*]")
                .getOrCreate();

        Map<String, String> options = new HashMap<>();
        options.put("url", "jdbc:mysql://localhost:3306/DB_HOPITAL");
        options.put("driver", "com.mysql.jdbc.Driver");
        options.put("user", "root");
        options.put("password", "");

        //Afficher le nombre de consultations par jour
        Dataset<Row> dfMysql1 = ss.read()
                .format("jdbc")
                .options(options)
                .option("query", "SELECT DATE(date_consultation) AS jour, COUNT(*) AS nombre_consultations FROM consultations GROUP BY DATE(date_consultation)")
                .load();

        dfMysql1.show();

        //Afficher le nombre de consultations par médecin

        Dataset<Row> dfMysql2 = ss.read()
                .format("jdbc")
                .options(options)
                .option("query", "SELECT m.NOM, m.PRENOM, COUNT(c.ID) AS nombre_consultation FROM MEDECINS m JOIN CONSULTATIONS c ON m.ID = c.ID_MEDECIN GROUP BY m.NOM, m.PRENOM")
                .load();

        dfMysql2.show();

        //Afficher pour chaque médecin, le nombre de patients qu’il a assisté

        Dataset<Row> dfMysql3 = ss.read()
                .format("jdbc")
                .options(options)
                .option("query", "SELECT m.NOM, m.PRENOM, COUNT(DISTINCT c.id_patient) AS nombre_patients_assistes FROM MEDECINS m JOIN CONSULTATIONS c ON m.ID = c.ID_MEDECIN GROUP BY m.NOM, m.PRENOM")
                .load();

        dfMysql3.show();
    }
}
