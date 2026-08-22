/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.splunkhec;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import org.opensearch.dataprepper.http.BaseHttpServerConfig;
import org.opensearch.dataprepper.plugins.source.splunkhec.model.HecTokenConfig;

import java.time.Duration;
import java.util.Collections;
import java.util.List;

public class SplunkHecSourceConfig extends BaseHttpServerConfig {

    static final int DEFAULT_PORT = 8088;
    static final String DEFAULT_PATH_PREFIX = "/services/collector";
    static final String DEFAULT_SOURCETYPE = "httpevent";
    static final String DEFAULT_RAW_LINE_BREAKER = "\n";
    static final Duration DEFAULT_ACK_EXPIRY = Duration.ofSeconds(300);

    @JsonProperty("tokens")
    @JsonPropertyDescription("The list of HEC tokens accepted on the Authorization: Splunk header. " +
            "At least one token is required. Each token may set per-token metadata defaults and may be disabled.")
    @NotEmpty(message = "At least one token must be configured")
    @Valid
    private List<HecTokenConfig> tokens = Collections.emptyList();

    @JsonProperty("flatten_event")
    @JsonPropertyDescription("When an event value is a JSON object, flatten its keys into top-level event fields. " +
            "When false, the object is kept nested under an event key. Defaults to true.")
    private boolean flattenEvent = true;

    @JsonProperty("raw_line_breaker")
    @JsonPropertyDescription("The literal delimiter used to split the body of the raw endpoint into one event per line. " +
            "Defaults to a newline.")
    @NotEmpty(message = "raw_line_breaker must not be empty")
    private String rawLineBreaker = DEFAULT_RAW_LINE_BREAKER;

    @JsonProperty("default_sourcetype")
    @JsonPropertyDescription("The sourcetype applied to events when neither the request nor the token defaults specify one. " +
            "Defaults to httpevent.")
    private String defaultSourcetype = DEFAULT_SOURCETYPE;

    @JsonProperty("warn_future_timestamps")
    @JsonPropertyDescription("When true, logs a warning for events whose timestamp is more than one hour in the future. " +
            "Defaults to false.")
    private boolean warnFutureTimestamps;

    @JsonProperty("acknowledgements")
    @JsonPropertyDescription("Enables the HEC indexer acknowledgement protocol so clients can poll delivery status. " +
            "Defaults to false.")
    private boolean acknowledgements;

    @JsonProperty("ack_expiry")
    @JsonPropertyDescription("How long acknowledgement state is retained before it is eligible for cleanup. " +
            "Defaults to 300 seconds.")
    @NotNull(message = "ack_expiry must not be null")
    private Duration ackExpiry = DEFAULT_ACK_EXPIRY;

    @JsonProperty("use_forwarded_headers")
    @JsonPropertyDescription("When true, trusts X-Forwarded-For for client IP logging behind a load balancer. " +
            "Defaults to false.")
    private boolean useForwardedHeaders;

    public List<HecTokenConfig> getTokens() {
        if (tokens == null) {
            return Collections.emptyList();
        }
        return Collections.unmodifiableList(tokens);
    }

    public boolean isFlattenEvent() {
        return flattenEvent;
    }

    public String getRawLineBreaker() {
        return rawLineBreaker;
    }

    public String getDefaultSourcetype() {
        return defaultSourcetype;
    }

    public boolean isWarnFutureTimestamps() {
        return warnFutureTimestamps;
    }

    public boolean isAcknowledgements() {
        return acknowledgements;
    }

    public Duration getAckExpiry() {
        return ackExpiry;
    }

    public boolean isUseForwardedHeaders() {
        return useForwardedHeaders;
    }

    @AssertTrue(message = "tokens must not contain null or blank token values")
    boolean isTokensValid() {
        return tokens != null
                && tokens.stream().allMatch(t -> t != null && t.getToken() != null && !t.getToken().isBlank());
    }

    @AssertTrue(message = "ack_expiry must be positive")
    boolean isAckExpiryPositive() {
        return ackExpiry == null || (!ackExpiry.isNegative() && !ackExpiry.isZero());
    }

    @Override
    public int getDefaultPort() {
        return DEFAULT_PORT;
    }

    @Override
    public String getDefaultPath() {
        return DEFAULT_PATH_PREFIX;
    }

    @Override
    public boolean hasHealthCheckService() {
        return false;
    }
}
