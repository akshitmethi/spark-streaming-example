package com.methi.mystreaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;

public class StreamingSalesDashBoard {

    public static void main(String[] args) throws TimeoutException, InterruptedException {
        //starting data generator
        KafkaDataGenerator dataGenerator = new KafkaDataGenerator();
        Thread generator = new Thread(dataGenerator);
        generator.start();

        //starting redisManager to print the leaderboard
        RedisManager redisManager = new RedisManager();
        redisManager.setup();
        Thread redis = new Thread(redisManager);
        redis.start();

        Logger.getLogger("org").setLevel(Level.WARN );
        Logger.getLogger("akka").setLevel(Level.WARN);

        System.out.println("Stating Streaming job");
        SparkSession spark = SparkSession
                .builder()
                .master("local[2]")
                .config("spark.driver.host","127.0.0.1")
                .config("spark.sql.shuffle.partitions",2)
                .config("spark.default.parallelism",2)
                .appName("StreamingLeaderboardToRedis")
                .getOrCreate();

        Dataset<Row> itemData = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers","localhost:29092")
                .option("subscribe","item.topic")
              //  .option("startingOffsets","earliest")
                .load();

        StructType jsonSchema = new StructType()
                .add("id", IntegerType)
                .add("name", StringType)
                .add("quantity",IntegerType)
                .add("price",IntegerType);


        itemData.selectExpr("cast(key as string) as key",
                "cast(value as string) as value")
                .select(functions.from_json(functions.col("value"),jsonSchema).as("item"))
                .selectExpr("item.id as id",
                        "item.name as name",
                        "item.quantity as quantity",
                        "item.price as price").writeStream().foreach(new RedisWriter()).start();

        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await();

    }

}
