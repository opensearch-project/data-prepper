/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.http.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;
import org.opensearch.dataprepper.model.configuration.PluginModel;

public class UrlConfigurationOption {

    private static final int DEFAULT_WORKERS = 1;

    @NotNull
    @JsonProperty("url")
    private String url;

    @JsonProperty("workers")
    private Integer workers = DEFAULT_WORKERS;

    @JsonProperty("proxy")
    private String proxy;

    @JsonProperty("codec")
    private PluginModel codec;

    @JsonProperty("http_method")
    private HTTPMethodOptions httpMethod;

    @JsonProperty("auth_type")
    private AuthTypeOptions authType;

    public String getUrl() {
        return url;
    }

    public Integer getWorkers() {
        return workers;
    }

    public String getProxy() {
        return proxy;
    }

    public PluginModel getCodec() {
        return codec;
    }

    public HTTPMethodOptions getHttpMethod() {
        return httpMethod;
    }

    public AuthTypeOptions getAuthType() {
        return authType;
    }

}
