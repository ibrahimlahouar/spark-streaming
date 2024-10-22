package org.example;

import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.errors.MinioException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.TimeUnit;

public class App {
    public static void main(String[] args) throws Exception {
        // Configuration de Spark
        SparkConf conf = new SparkConf().setAppName("FlightDataApp").setMaster("local[*]");
        conf.set("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000");
        conf.set("spark.hadoop.fs.s3a.access.key", "minioadmin");
        conf.set("spark.hadoop.fs.s3a.secret.key", "minioadmin");
        conf.set("spark.hadoop.fs.s3a.path.style.access", "true");


        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        // Configuration MinIO
        String minioUrl = "http://localhost:9000";
        String accessKey = "minioadmin";
        String secretKey = "minioadmin";
        String bucketName = "minio-bucket";

        // Initialisation du client MinIO
        MinioClient minioClient = MinioClient.builder()
                .endpoint(minioUrl)
                .credentials(accessKey, secretKey)
                .build();

        // Vérification et création du bucket
        try {
            boolean found = minioClient.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build());
            if (!found) {
                System.out.println("Bucket '" + bucketName + "' does not exist. Creating...");
                minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
                System.out.println("Bucket '" + bucketName + "' created successfully.");
            } else {
                System.out.println("Bucket '" + bucketName + "' already exists.");
            }
        } catch (MinioException e) {
            System.err.println("Error occurred: " + e);
        }

        // Date de début à partir de juin 2024
        LocalDateTime startDate = LocalDateTime.of(2024, 6, 1, 0, 0);
        long startEpoch = startDate.toEpochSecond(ZoneOffset.UTC);

        // Boucle pour effectuer des appels API toutes les 2 heures
        while (true) {
            long begin = startEpoch;
            long end = begin + 7200; // 2 hours in seconds

            // Configuration de l'URL de l'API
            String apiUrl = String.format("https://opensky-network.org/api/flights/all?begin=%d&end=%d", begin, end);

            // Lecture des données de l'API avec Spark
            Dataset<Row> flightData = spark.read().format("json").load(apiUrl);

            // Stockage des données dans MinIO
            String outputPath = String.format("s3a://%s/flight-data/%d", bucketName, begin);
            flightData.write()
                .format("parquet")
                .mode("overwrite")
                .save(outputPath);

            System.out.println("Data successfully written to MinIO for interval: " + begin + " to " + end);

            // Attendre 2 heures avant le prochain appel
            TimeUnit.MINUTES.sleep(2);

            // Mettre à jour le début pour le prochain intervalle
            startEpoch = end;
        }
    }
}