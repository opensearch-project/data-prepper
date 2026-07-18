/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rss.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;

public class AuthenticationConfig {

    @JsonProperty("basic")
    @Valid
    private BasicAuthConfig basic;

    public BasicAuthConfig getBasic() {
        return basic;
    }
}
