package com.poc.spark.etl.job;

import com.poc.spark.etl.model.ExtractedData;
import com.poc.spark.etl.model.Record;
import com.poc.spark.etl.template.IngestionTemplate;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.Map;

@Slf4j
public class JobExecutor extends IngestionTemplate implements Serializable {

    public final void startJobProcessing(String processId,Broadcast<Map<String, String>> broadcastMap,String filePath) {
        Dataset<Record> recordDataset = loadDataFromS3(filePath,processId);
        Dataset<String> stringDataset = recordDataset.select("recordType").distinct().as(Encoders.STRING());
        Dataset<String> stringRecords = stringDataset.select("row").where(stringDataset.col("recordType").notEqual("1").and(stringDataset.col("recordType").notEqual("3"))).as(Encoders.STRING());
        try {
            loadHeader(stringDataset,processId);
            loadFooter(stringDataset,processId);
            Dataset<ExtractedData> ingestionDataset = encodeToModel(stringRecords,processId);
            sendToPublisher(ingestionDataset,processId,broadcastMap);
        } catch (Exception e) {
            log.error(e.getMessage(),e);
            SparkSession.getActiveSession().get().sparkContext().stop();
        }
    }

}
