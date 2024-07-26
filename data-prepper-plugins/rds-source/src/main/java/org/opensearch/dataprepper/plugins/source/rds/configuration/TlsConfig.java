/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TlsConfig {

    @JsonProperty("insecure")
    private boolean insecure = false;

    // TODO: add options for server identity verification

    public boolean isInsecure() {
        return insecure;
    }
}
