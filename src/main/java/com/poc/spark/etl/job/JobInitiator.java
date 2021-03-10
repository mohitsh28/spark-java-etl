package com.poc.spark.etl.job;

import com.poc.spark.etl.configurations.ConfigurationLoader;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.Map;

@Slf4j
public class JobInitiator implements Serializable {
    private final JobExecutor jobExecutor = new JobExecutor();

    public final SparkSession createSparkSession() {
        return SparkSession.builder()
                .config("spark.hadoop.fs.s3a.endpoint","s3.eu-west-1.amazonaws.com")
                .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version","2")
                .config("spark.hadoop.fs.s3a.multiobjectdelete.enable","false")
                .config("spark.hadoop.fs.s3a.fast.upload","true")
                .config("spark.speculation","false")
                .getOrCreate();
    }

    public void startInitiation(String configurationURL,String env,String processId,String filePath) {
        log.info("Creating Spark Session {0}",processId);
        SparkSession sparkSession = createSparkSession();
        JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
        log.info("Loading Properties {0}",processId);
        Map propertiesMap = ConfigurationLoader.getApplicationPropertiesMap(configurationURL,env);
        Broadcast<Map<String, String>> broadcastProperty = javaSparkContext.broadcast(propertiesMap);
        try {
            log.info("Starting job processor {0}",processId);
            jobExecutor.startJobProcessing(processId,broadcastProperty,filePath);
        } catch (Exception e) {
            log.error(e.getMessage(),e);
        } finally {
            SparkSession.getActiveSession().get().sparkContext().stop();
        }
    }
}
