/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.coordination.state;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ExportProgressState {

    @JsonProperty("exportArn")
    private String exportArn;

    @JsonProperty("status")
    private String status;

    @JsonProperty("bucket")
    private String bucket;

    @JsonProperty("prefix")
    private String prefix;

    @JsonProperty("kmsKeyId")
    private String kmsKeyId;

    @JsonProperty("exportTime")
    private String exportTime;


    public String getExportArn() {
        return exportArn;
    }

    public void setExportArn(String exportArn) {
        this.exportArn = exportArn;
    }

    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    public String getExportTime() {
        return exportTime;
    }

    public void setExportTime(String exportTime) {
        this.exportTime = exportTime;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getKmsKeyId() {
        return kmsKeyId;
    }

    public void setKmsKeyId(String kmsKeyId) {
        this.kmsKeyId = kmsKeyId;
    }
}
