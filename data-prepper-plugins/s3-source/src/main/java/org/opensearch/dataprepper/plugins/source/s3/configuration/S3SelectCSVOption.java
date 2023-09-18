/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.s3.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

public class S3SelectCSVOption {
    static final String DEFAULT_CSV_HEADER = "USE";
    @JsonProperty("file_header_info")
    private String fileHeaderInfo = DEFAULT_CSV_HEADER;
    @JsonProperty("quote_escape")
    private String quiteEscape;
    @JsonProperty("comments")
    private String comments;

    public String getFileHeaderInfo() {
        return fileHeaderInfo;
    }

    public String getQuiteEscape() {
        return quiteEscape;
    }

    public String getComments() {
        return comments;
    }
}
