/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
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
