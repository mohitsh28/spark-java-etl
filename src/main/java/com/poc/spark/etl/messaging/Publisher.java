package com.poc.spark.etl.messaging;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;

import java.util.Map;

@Slf4j
public class Publisher {

    public static void publishMessage(Dataset messageDataSet,String processId,Broadcast<Map<String, String>> broadcastMap) {
        log.info("Encode Extracted data to model {0}",processId);
        String kafkaTopic;
        kafkaTopic = broadcastMap.getValue().get("topic");

        messageDataSet.toDF("id","value").selectExpr("CAST(id AS STRING) AS key","CAST(value AS STRING)")
                .write()
                .format("kafka")
                .option("topic",kafkaTopic)
                .option("kafka.bootstrap.servers",broadcastMap.getValue().get("kafka.bootstrap.servers"))
                .save();
    }
}
