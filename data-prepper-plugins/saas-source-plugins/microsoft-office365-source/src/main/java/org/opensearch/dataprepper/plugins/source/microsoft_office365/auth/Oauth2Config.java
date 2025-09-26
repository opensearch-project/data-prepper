/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.microsoft_office365.auth;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.AssertTrue;
import lombok.Getter;
import org.opensearch.dataprepper.model.plugin.PluginConfigVariable;

@Getter
public class Oauth2Config {
    @JsonProperty("client_id")
    private PluginConfigVariable clientId;

    @JsonProperty("client_secret")
    private PluginConfigVariable clientSecret;
}
