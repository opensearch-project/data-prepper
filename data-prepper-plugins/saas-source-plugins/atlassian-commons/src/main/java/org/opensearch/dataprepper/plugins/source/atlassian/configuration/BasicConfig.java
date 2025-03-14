/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.atlassian.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.AssertTrue;
import lombok.Getter;

@Getter
public class BasicConfig {
    @JsonProperty("username")
    private String username;

    @JsonProperty("password")
    private String password;

    @AssertTrue(message = "Username and Password are both required for Basic Auth")
    private boolean isBasicConfigValid() {
        return username != null && password != null;
    }
}
