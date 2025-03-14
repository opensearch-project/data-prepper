/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source;

import com.fasterxml.jackson.annotation.JsonProperty;

public class StdInSourceConfig {
    private static final int WRITE_TIMEOUT = 5_000;

    @JsonProperty("write_timeout")
    private int writeTimeout = WRITE_TIMEOUT;

    public int getWriteTimeout() {
        return writeTimeout;
    }
}
