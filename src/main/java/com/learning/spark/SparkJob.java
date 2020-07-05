package com.learning.spark;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.*;
import java.util.regex.Pattern;

public class SparkJob {

    private static final Pattern SPACE = Pattern.compile(" ");
    private static final Logger logger = LoggerFactory.getLogger(SparkJob.class);
    private static final String readMe = "/usr/local/Cellar/apache-spark/2.4.4/README.md";
    private static final String reqInvoiceDto = "/Users/srikanths/Downloads/reqInvoiceDto.json";
    private static final String INVOICE_TOPIC = "oem_plutus_invoice";

    public static void main(String[] args) throws InterruptedException {
        readFromKafka();
//        SparkSession spark = SparkSession.builder().appName("Learning Spark Job").getOrCreate();
//        runWordCountJob(spark);
//        runShowSql(spark);
//        spark.stop();
    }

    private static void readFromKafka() throws InterruptedException {
        JavaStreamingContext jssc = getStreamingContext();
        Map<String, Object> kafkaParams = getKafkaParams();
        Collection<String> topics = Arrays.asList(INVOICE_TOPIC);
        logger.info("Before printing kafka stream");

        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );
        JavaDStream<Tuple2<String,String>> kafkaStream = stream.map(record -> new Tuple2<>(record.key(), record.value()));
        kafkaStream.print();
        jssc.start();              // Start the computation
        jssc.awaitTermination();   // Wait for the computation to terminate
    }

    private static JavaStreamingContext getStreamingContext() {
        // Create a local StreamingContext with two working thread and batch interval of 1 second
        SparkConf conf = new SparkConf().setAppName("Read from Kafka Job");
        return new JavaStreamingContext(conf, Durations.seconds(1));
    }

    private static Map getKafkaParams() {

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", INVOICE_TOPIC+"_group");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);
        return kafkaParams;
    }

    private static void runWordCountJob(SparkSession spark) {
        JavaRDD<String> readMeDataset = spark.read().textFile(readMe).javaRDD();

        JavaRDD<String> words = readMeDataset.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());

        JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));

        JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);

        List<Tuple2<String, Integer>> output = counts.collect();
        for (Tuple2<?,?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }
    }

    public static void runShowSql(SparkSession spark){
        Dataset<Row> reqInvoice = spark.read().json(reqInvoiceDto);
        logger.info("Show ReqInvoiceDto Json");
        reqInvoice.show();
        logger.info("Show ReqInvoiceDto in Tree format");
        reqInvoice.printSchema();
    }
}
