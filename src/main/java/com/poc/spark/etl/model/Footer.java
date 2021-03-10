package com.poc.spark.etl.model;

import lombok.*;

import java.io.Serializable;

@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class Footer implements Serializable {
    private static final long serialVersionUID = 3085037606801175294L;
    private String recordType;
    private String rowData;
}