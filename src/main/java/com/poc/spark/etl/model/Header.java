package com.poc.spark.etl.model;

import lombok.*;

import java.io.Serializable;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
@ToString
public class Header implements Serializable {
    private static final long serialVersionUID = 3518936004744924915L;
    private String recordType;
    private String rowData;

}

