/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.prometheus;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.opensearch.dataprepper.http.BaseHttpServerConfig;

/**
 * Configuration for the Prometheus Remote Write Source.
 * This source accepts Prometheus Remote Write protocol requests and converts them to Data Prepper events.
 */
public class PrometheusRemoteWriteSourceConfig extends BaseHttpServerConfig {

    static final String DEFAULT_REMOTE_WRITE_URI = "/api/v1/write";
    static final int DEFAULT_PORT = 9090;

    @JsonProperty("flatten_labels")
    private boolean flattenLabels = false;

    @Override
    public int getDefaultPort() {
        return DEFAULT_PORT;
    }

    @Override
    public String getDefaultPath() {
        return DEFAULT_REMOTE_WRITE_URI;
    }

    /**
     * Whether to flatten labels into individual fields instead of a map.
     *
     * @return true if labels should be flattened
     */
    public boolean isFlattenLabels() {
        return flattenLabels;
    }
}
