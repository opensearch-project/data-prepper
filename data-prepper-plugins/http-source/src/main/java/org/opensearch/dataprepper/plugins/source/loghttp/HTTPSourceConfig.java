/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.loghttp;

import org.opensearch.dataprepper.http.BaseHttpServerConfig;

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
}
