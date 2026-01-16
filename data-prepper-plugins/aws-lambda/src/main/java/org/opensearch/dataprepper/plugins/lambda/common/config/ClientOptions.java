package org.opensearch.dataprepper.plugins.lambda.common.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import lombok.Getter;

import java.time.Duration;

@Getter
public class ClientOptions {
    public static final int DEFAULT_CONNECTION_RETRIES = 3;
    public static final int DEFAULT_MAXIMUM_CONCURRENCY = 200;
    public static final Duration DEFAULT_CONNECTION_TIMEOUT = Duration.ofSeconds(60);
    public static final Duration DEFAULT_READ_TIMEOUT = Duration.ofSeconds(60);
    public static final Duration DEFAULT_API_TIMEOUT = Duration.ofSeconds(60);
    public static final Duration DEFAULT_BASE_DELAY = Duration.ofMillis(100);
    public static final Duration DEFAULT_MAX_BACKOFF = Duration.ofSeconds(20);

    @JsonPropertyDescription("Total retries we want before failing")
    @JsonProperty("max_retries")
    private int maxConnectionRetries = DEFAULT_CONNECTION_RETRIES;

    @JsonPropertyDescription("api call timeout defines the time sdk maintains the api call to complete before timing out")
    @JsonProperty("api_call_timeout")
    private Duration apiCallTimeout = DEFAULT_API_TIMEOUT;

    @JsonPropertyDescription("sdk timeout defines the time sdk maintains the connection to the client before timing out")
    @JsonProperty("connection_timeout")
    private Duration connectionTimeout = DEFAULT_CONNECTION_TIMEOUT;

    @JsonPropertyDescription("read timeout defines the time sdk waits for data to be read from an established connection")
    @JsonProperty("read_timeout")
    private Duration readTimeout = DEFAULT_READ_TIMEOUT;

    @JsonPropertyDescription("max concurrency defined from the client side")
    @JsonProperty("max_concurrency")
    private int maxConcurrency = DEFAULT_MAXIMUM_CONCURRENCY;

    @JsonPropertyDescription("Base delay for exponential backoff")
    @JsonProperty("base_delay")
    private Duration baseDelay = DEFAULT_BASE_DELAY;

    @JsonPropertyDescription("Maximum backoff time for exponential backoff")
    @JsonProperty("max_backoff")
    private Duration maxBackoff = DEFAULT_MAX_BACKOFF;

}