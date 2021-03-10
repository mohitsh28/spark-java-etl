package com.poc.spark.etl;

import com.poc.spark.etl.job.JobInitiator;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;

import java.util.UUID;

@Slf4j
public class AppRunner {

    public static void main(String[] args) {
        String processId = UUID.randomUUID().toString();
        String env = "local";
        String configURL = "localhost:8080";
        String filePath = "";
        try {
            JobInitiator jobInitiator = new JobInitiator();
            jobInitiator.startInitiation(configURL,env,processId,filePath);
        } catch (Exception e) {
            log.error(e.getMessage(),e);
        } finally {
            log.info("ETL job is completed.");
            SparkSession.getActiveSession().get().sparkContext().stop();
        }
    }
}
