package com.poc.spark.etl.model;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class Record {
    private String recordType;
    private String row;
    private int length;

    public Record(String recordType,String row,int length) {
        this.recordType = recordType;
        this.row = row;
        this.length = length;
    }
}
