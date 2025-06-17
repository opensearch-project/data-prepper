/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.loghttp;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.opensearch.dataprepper.http.BaseHttpServerConfig;
import org.opensearch.dataprepper.model.configuration.PluginModel;

public class HTTPSourceConfig extends BaseHttpServerConfig {

    static final String DEFAULT_LOG_INGEST_URI = "/log/ingest";
    static final int DEFAULT_PORT = 2021;

    @Override
    public int getDefaultPort() {
        return DEFAULT_PORT;
    }

    @Override
    public String getDefaultPath() {
        return DEFAULT_LOG_INGEST_URI;
    }

    @JsonProperty("codec")
    private PluginModel codec;

    public PluginModel getCodec() {
        return codec;
    }
}
