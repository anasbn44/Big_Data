package ma;

import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {
    public static void main(String[] args) {
        SparkSession ss = SparkSession.builder().appName("spark_ml").master("local[*]").getOrCreate();

        Dataset<Row> dataset = ss.read().option("inferSchema",true).option("header",true).csv("advertising.csv");

        VectorAssembler assembler = new VectorAssembler().setInputCols(new String[]{"TV","Radio","Newspaper"}).setOutputCol("Features");
        Dataset<Row> assembleDs = assembler.transform(dataset);
        Dataset<Row> split[] = assembleDs.randomSplit(new double[]{0.8, 0.2}, 42);
        Dataset<Row> train = split[0];
        Dataset<Row> test = split[1];

        LinearRegression lr = new LinearRegression().setLabelCol("Sales").setFeaturesCol("Features");
        LinearRegressionModel model = lr.fit(train);
        Dataset<Row> tests = model.transform(test);
        tests.show();
    }
}