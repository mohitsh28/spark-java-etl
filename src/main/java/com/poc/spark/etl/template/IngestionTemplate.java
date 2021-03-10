package com.poc.spark.etl.template;

import com.poc.spark.etl.messaging.Publisher;
import com.poc.spark.etl.model.ExtractedData;
import com.poc.spark.etl.model.Footer;
import com.poc.spark.etl.model.Header;
import com.poc.spark.etl.model.Record;
import com.poc.spark.etl.model.encoders.ModelEncoders;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.Map;

@Slf4j
public abstract class IngestionTemplate implements Serializable {
    private final SparkSession sparkSession = SparkSession.getActiveSession().get();
    private final ModelEncoders modelEncoders = new ModelEncoders();

    public final Dataset<Record> loadDataFromS3(String path,String processId) {
        log.info("Fetching file from S3 bucket {0}",processId);
        return sparkSession.read()
                .textFile(path)
                .toDF()
                .map((MapFunction<Row, Record>) r -> new Record(r.getString(0).substring(0,1),r.getString(0),r.getString(0).length())
                        ,Encoders.bean(Record.class));
    }

    public final Dataset<Header> loadHeader(Dataset stringDataset,String processId) throws Exception {
        Dataset<Header> headerDataset;
        log.info("Fetching header values {0}",processId);
        if (!stringDataset.filter(stringDataset.col("recordType").equalTo("1")).isEmpty()) {
            headerDataset = stringDataset.filter((FilterFunction<Record>) r -> r.getRecordType().trim().equals("1")).select("row").as(Encoders.STRING());
        } else
            throw new Exception("Invalid Header");
        return headerDataset;
    }

    public final Dataset<Footer> loadFooter(Dataset stringDataset,String processId) throws Exception {
        Dataset<Footer> footerDataset;
        log.info("Fetching footer values {0}",processId);
        if (!stringDataset.filter(stringDataset.col("recordType").equalTo("3")).isEmpty()) {
            footerDataset = stringDataset.filter((FilterFunction<Record>) r -> r.getRecordType().trim().equals("3")).select("row").as(Encoders.STRING());
        } else
            throw new Exception("Invalid Footer");
        return footerDataset;
    }

    public final Dataset<ExtractedData> encodeToModel(Dataset stringRecords,String processId) {
        log.info("Encode Extracted data to model {0}",processId);
        return modelEncoders.create(stringRecords);
    }

    public final void sendToPublisher(Dataset stringRecords,String processId,Broadcast<Map<String, String>> broadcastMap) {
        log.info("Publishing to Kafka {0}",processId);
        Publisher.publishMessage(stringRecords,processId,broadcastMap);
    }


}
