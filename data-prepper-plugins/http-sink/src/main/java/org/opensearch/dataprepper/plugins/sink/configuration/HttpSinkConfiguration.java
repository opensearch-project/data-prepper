/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import org.opensearch.dataprepper.model.configuration.PluginModel;

import java.util.List;

public class HttpSinkConfiguration {

    private static final int DEFAULT_UPLOAD_RETRIES = 5;

    private static final String DEFAULT_HTTP_METHOD = "POST";

    private static final int DEFAULT_WORKERS = 1;

    @NotNull
    @JsonProperty("urls")
    private List<UrlConfigurationOption> urlConfigurationOptions;

    @JsonProperty("workers")
    private Integer workers = DEFAULT_WORKERS;

    @JsonProperty("codec")
    private PluginModel codec;

    @JsonProperty("http_method")
    private String httpMethod = DEFAULT_HTTP_METHOD;

    @JsonProperty("proxy")
    private String proxy;

    @JsonProperty("auth_type")
    private String authType;

    private PluginModel authentication;

    @JsonProperty("insecure")
    private boolean insecure;

    @JsonProperty("ssl_certificate_file")
    private String sslCertificateFile;

    @JsonProperty("ssl_key_file")
    private String sslKeyFile;

    @JsonProperty("aws_sigv4")
    private boolean awsSigv4;

    @JsonProperty("buffer_type")
    //private BufferTypeOptions bufferType = BufferTypeOptions.INMEMORY;
    private String  bufferType = "in_memory";  //TODO: change to BufferTypeOptions

    @JsonProperty("threshold")
    private ThresholdOptions thresholdOptions;

    @JsonProperty("max_retries")
    private int maxUploadRetries = DEFAULT_UPLOAD_RETRIES;

    @JsonProperty("aws")
    @Valid
    private AwsAuthenticationOptions awsAuthenticationOptions;

    @JsonProperty("custom_header")
    private CustomHeaderOptions customHeaderOptions;

    @JsonProperty("dlq_file")
    private String dlqFile;

    private PluginModel dlq;

    public List<UrlConfigurationOption> getUrlConfigurationOptions() {
        return urlConfigurationOptions;
    }

    public PluginModel getCodec() {
        return codec;
    }

    public String getHttpMethod() {
        return httpMethod;
    }

    public String getProxy() {
        return proxy;
    }

    public String getAuthType() {
        return authType;
    }

    public PluginModel getAuthentication() {
        return authentication;
    }

    public boolean isInsecure() {
        return insecure;
    }

    public String getSslCertificateFile() {
        return sslCertificateFile;
    }

    public String getSslKeyFile() {
        return sslKeyFile;
    }

    public boolean isAwsSigv4() {
        return awsSigv4;
    }

    public String getBufferType() {
        return bufferType;
    }

    public ThresholdOptions getThresholdOptions() {
        return thresholdOptions;
    }

    public int getMaxUploadRetries() {
        return maxUploadRetries;
    }

    public CustomHeaderOptions getCustomHeaderOptions() {
        return customHeaderOptions;
    }

    public AwsAuthenticationOptions getAwsAuthenticationOptions() {
        return awsAuthenticationOptions;
    }

    public Integer getWorkers() {
        return workers;
    }

    public String getDlqFile() {
        return dlqFile;
    }

    public PluginModel getDlq() {
        return dlq;
    }
}
