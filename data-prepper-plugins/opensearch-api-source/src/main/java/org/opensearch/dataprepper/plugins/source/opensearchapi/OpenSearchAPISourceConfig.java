/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearchapi;

import org.opensearch.dataprepper.http.BaseHttpServerConfig;

public class OpenSearchAPISourceConfig extends BaseHttpServerConfig {

    static final String DEFAULT_ENDPOINT_URI = "/opensearch";
    static final int DEFAULT_PORT = 9202;

    @Override
    public int getDefaultPort() {
        return DEFAULT_PORT;
    }

    @Override
    public String getDefaultPath() {
        return DEFAULT_ENDPOINT_URI;
    }
}
