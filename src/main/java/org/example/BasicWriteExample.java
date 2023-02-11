package org.example;

import com.mongodb.spark.sql.connector.config.WriteConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.time.Instant;

import static org.apache.spark.sql.functions.current_timestamp;

public class BasicWriteExample {
    private static final String relativePath = "./src/main/resources/";

    public static void main(String[] args) {
        Instant begin = Instant.now();
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("spark session")
                .getOrCreate();

        Dataset<Row> user = spark.read()
                                 .option("recursiveFileLookup", "true")
                                 .parquet(relativePath);

        user = user.drop("first_name");
        user = user.withColumn("updated_on", current_timestamp());
        String connectionUri = "mongodb://localhost:27017";
        user.write()
            .format("mongodb")
            .mode("append")
            .option("database", "spark")
            .option("collection", "basic_user")
            .option("connection.uri", connectionUri)
            .option(WriteConfig.OPERATION_TYPE_CONFIG, "update") // turn this on to allow partial update
            .option(WriteConfig.ID_FIELD_CONFIG, "id") // unique id
            .option(WriteConfig.ORDERED_BULK_OPERATION_CONFIG, "false")
            .save();

        Instant end = Instant.now();
        System.out.println("Time taken in seconds: " + (end.getEpochSecond() - begin.getEpochSecond()));
    }
}