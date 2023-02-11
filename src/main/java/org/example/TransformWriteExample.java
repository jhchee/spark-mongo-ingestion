package org.example;

import com.mongodb.spark.sql.connector.config.WriteConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import java.time.Instant;

import static org.apache.spark.sql.functions.*;

public class TransformWriteExample {
    private static final String relativePath = "./src/main/resources/";

    public static void main(String[] args) {
        Instant begin = Instant.now();
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]") // all cores
                .appName("spark session")
                .getOrCreate();

        Dataset<Row> user = spark.read()
                                 .option("recursiveFileLookup", "true")
                                 .parquet(relativePath);

        user = user.select(
            struct("first_name", "last_name").as("name"),
            col("id").as("id"),
            col("registration_dttm").as("registration_dttm"),
            col("first_name").as("first_name"),
            col("last_name").as("last_name"),
            col("email").as("email"),
            col("gender").as("gender"),
            col("ip_address").as("ip_address"),
            col("cc").as("cc"),
            col("country").as("country"),
            col("birthdate").as("birthdate"),
            col("salary").as("salary"),
            col("title").as("title"),
            col("comments").as("comments")
        );
        user = user.withColumn("updated_on", current_timestamp());
        String connectionUri = "mongodb://localhost:27017";
        user.write()
            .format("mongodb")
            .mode("append")
            .option("database", "spark")
            .option("collection", "transformed_user")
            .option("connection.uri", connectionUri)
            .option(WriteConfig.OPERATION_TYPE_CONFIG, "update") // turn this on to allow partial update
            .option(WriteConfig.ID_FIELD_CONFIG, "id") // unique id
            .option(WriteConfig.ORDERED_BULK_OPERATION_CONFIG, "false")
            .save();

        Instant end = Instant.now();
        System.out.println("Time taken in seconds: " + (end.getEpochSecond() - begin.getEpochSecond()));
    }

}