/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.codec.csv;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Size;

import java.util.List;

@JsonPropertyOrder
@JsonClassDescription("The `csv` processor parses comma-separated values (CSVs) from the event into columns.")
public class CsvOutputCodecConfig {
    static final String DEFAULT_DELIMITER = ",";

    @JsonProperty("delimiter")
    private String delimiter = DEFAULT_DELIMITER;

    @JsonProperty("header")
    private List<String> header;

    @Valid
    @Size(max = 0, message = "Header from file is not supported.")
    @JsonProperty("header_file_location")
    private String headerFileLocation;

    @Valid
    @Size(max = 0, message = "Header from file is not supported.")
    @JsonProperty("region")
    private String region;
    @Valid
    @Size(max = 0, message = "Header from file is not supported.")
    @JsonProperty("bucket_name")
    private String bucketName;
    @Valid
    @Size(max = 0, message = "Header from file is not supported.")
    @JsonProperty("fileKey")
    private String file_key;

    public String getHeaderFileLocation() {
        return headerFileLocation;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public List<String> getHeader() {
        return header;
    }

    public void setHeader(List<String> header) {
        this.header = header;
    }

    public String getRegion() {
        return region;
    }

    public String getBucketName() {
        return bucketName;
    }

    public String getFile_key() {
        return file_key;
    }

}