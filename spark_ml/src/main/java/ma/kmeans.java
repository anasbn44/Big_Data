package ma;

import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import org.apache.spark.ml.feature.MinMaxScaler;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class kmeans {
    public static void main(String[] args) {
        SparkSession ss = SparkSession.builder()
                .appName("spark_ml")
                .master("local[*]")
                .getOrCreate();
        Dataset<Row> dataset = ss.read()
                .option("inferSchema",true)
                .option("header",true)
                .csv("Mall_Customers.csv");

        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String[]{"Age","Annual Income (k$)","Spending Score (1-100)"})
                .setOutputCol("Features");

        Dataset<Row> transformedDataSet = assembler.transform(dataset);
        MinMaxScaler scaler = new MinMaxScaler()
                .setInputCol("Features")
                .setOutputCol("NormalizedFeatures");
        Dataset<Row> scaledDataSet = scaler
                .fit(transformedDataSet)
                .transform(transformedDataSet);
        KMeans kMeans = new KMeans()
                .setK(5)
                .setSeed(42)
                .setFeaturesCol("NormalizedFeatures")
                .setPredictionCol("Clusters");
        KMeansModel model = kMeans.fit(scaledDataSet);
        Dataset<Row> predictions = model.transform(scaledDataSet);
        predictions.show();

        ClusteringEvaluator evaluator = new ClusteringEvaluator()
                .setFeaturesCol("NormalizedFeatures")
                .setPredictionCol("Clusters");
        double evaluate = evaluator.evaluate(predictions);

        System.out.println("Clustering Evaluator Score : " + evaluate);
    }
}
