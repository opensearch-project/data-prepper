/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SecurityLakeBucketSelectorConfig {
    static final String DEFAULT_SOURCE_VERSION = "1.0";

    static final String DEFAULT_EXTERNAL_ID = "extid";

    @JsonProperty("source_name")
    private String sourceName;

    @JsonProperty("source_version")
    private String sourceVersion = DEFAULT_SOURCE_VERSION;

    @JsonProperty("external_id")
    private String externalId = DEFAULT_EXTERNAL_ID;

    @JsonProperty("log_class")
    private String logClass;


    public String getSourceName() {
        return sourceName;
    }

    public String getSourceVersion() {
        return sourceVersion;
    }

    public String getExternalId() {
        return externalId;
    }

    public String getLogClass() {
        return logClass;
    }

}
