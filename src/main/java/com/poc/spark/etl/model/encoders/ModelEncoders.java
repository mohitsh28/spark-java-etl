package com.poc.spark.etl.model.encoders;

import com.poc.spark.etl.model.ExtractedData;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;

public class ModelEncoders {

    public Dataset create(Dataset dataSet) {
        return dataSet.map(
                (MapFunction<String, ExtractedData>) row -> createModelObject(row),Encoders.bean(ExtractedData.class));
    }

    public ExtractedData createModelObject(String row) {
        ExtractedData model = new ExtractedData();
        String data = row;
        model.setRecordType(data.substring(0,9));
        model.setFName(data.substring(10,19));
        model.setLName(data.substring(20,29));
        model.setEmail(data.substring(30,49));
        return model;
    }
}
