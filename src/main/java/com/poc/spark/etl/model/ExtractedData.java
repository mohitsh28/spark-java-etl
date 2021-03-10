package com.poc.spark.etl.model;

import lombok.*;

import java.io.Serializable;
import java.sql.Timestamp;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
@EqualsAndHashCode

public class ExtractedData implements Serializable {
    private static final long serialVersionUID = -2559741144048958927L;
    private String recordType;
    private String recordId;

    private String fName;
    private String lName;
    private String phone;
    private String email;
    private String address;

    private String userCreated;
    private Timestamp dateCreated;
}
