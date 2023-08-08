package org.opensearch.dataprepper.plugins.kafka.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Duration;
import java.util.List;

public class KafkaProducerProperties {
    private static final String DEFAULT_BYTE_CAPACITY = "50mb";

    @JsonProperty("buffer_memory")
    private String bufferMemory = DEFAULT_BYTE_CAPACITY;

    @JsonProperty("compression_type")
    private String compressionType;

    @JsonProperty("retries")
    private int retries;

    @JsonProperty("batch_size")
    private int batchSize;

    @JsonProperty("client_dns_lookup")
    private String clientDnsLookup;

    @JsonProperty("client_id")
    private String clientId;

    @JsonProperty("connections_max_idle")
    private Duration connectionsMaxIdleMs;

    @JsonProperty("delivery_timeout")
    private Duration deliveryTimeoutMs;

    @JsonProperty("linger_ms")
    private Long lingerMs;

    @JsonProperty("max_block")
    private Duration maxBlockMs;

    @JsonProperty("max_request_size")
    private int maxRequestSize;

    @JsonProperty("partitioner_class")
    private Class partitionerClass;

    @JsonProperty("partitioner_ignore_keys")
    private Boolean partitionerIgnoreKeys;

    @JsonProperty("receive_buffer")
    private String receiveBufferBytes=DEFAULT_BYTE_CAPACITY;

    @JsonProperty("request_timeout")
    private Duration requestTimeoutMs;

    @JsonProperty("send_buffer")
    private String sendBufferBytes=DEFAULT_BYTE_CAPACITY;

    @JsonProperty("socket_connection_setup_timeout_max")
    private Duration socketConnectionSetupMaxTimeout;

    @JsonProperty("socket_connection_setup_timeout")
    private Duration socketConnectionSetupTimeout;

    @JsonProperty("acks")
    private String acks;

    @JsonProperty("enable_idempotence")
    private Boolean enableIdempotence;

    @JsonProperty("interceptor_classes")
    private List interceptorClasses;

    @JsonProperty("max_in_flight_requests_per_connection")
    private int maxInFlightRequestsPerConnection;

    @JsonProperty("metadata_max_age")
    private Duration metadataMaxAgeMs;

    @JsonProperty("metadata_max_idle")
    private Duration metadataMaxIdleMs;

    @JsonProperty("metric_reporters")
    private List metricReporters;

    @JsonProperty("metrics_num_samples")
    private int metricsNumSamples;

    @JsonProperty("metrics_recording_level")
    private String metricsRecordingLevel;

    @JsonProperty("metrics_sample_window")
    private Duration metricsSampleWindowMs;

    @JsonProperty("partitioner_adaptive_partitioning_enable")
    private boolean partitionerAdaptivePartitioningEnable;

    @JsonProperty("partitioner_availability_timeout")
    private Duration partitionerAvailabilityTimeoutMs;

    @JsonProperty("reconnect_backoff_max")
    private Duration reconnectBackoffMaxMs;

    @JsonProperty("reconnect_backoff")
    private Duration reconnectBackoffMs;

    @JsonProperty("retry_backoff")
    private Duration retryBackoffMs;


    public String getCompressionType() {
        return compressionType;
    }

    public int getRetries() {
        if (retries == 0) {
            retries = 5;
        }
        return retries;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public String getClientDnsLookup() {
        return clientDnsLookup;
    }

    public String getClientId() {
        return clientId;
    }


    public Long getLingerMs() {
        return lingerMs;
    }


    public int getMaxRequestSize() {
        return maxRequestSize;
    }

    public Class getPartitionerClass() {
        return partitionerClass;
    }

    public Boolean getPartitionerIgnoreKeys() {
        return partitionerIgnoreKeys;
    }


    public String getAcks() {
        return acks;
    }

    public Boolean getEnableIdempotence() {
        return enableIdempotence;
    }

    public List getInterceptorClasses() {
        return interceptorClasses;
    }

    public int getMaxInFlightRequestsPerConnection() {
        return maxInFlightRequestsPerConnection;
    }


    public List getMetricReporters() {
        return metricReporters;
    }

    public int getMetricsNumSamples() {
        return metricsNumSamples;
    }

    public String getMetricsRecordingLevel() {
        return metricsRecordingLevel;
    }


    public boolean isPartitionerAdaptivePartitioningEnable() {
        return partitionerAdaptivePartitioningEnable;
    }

    public String getBufferMemory() {
        return bufferMemory;
    }

    public Long getConnectionsMaxIdleMs() {
        return connectionsMaxIdleMs.toMillis();
    }

    public Long getDeliveryTimeoutMs() {
        return deliveryTimeoutMs.toMillis();
    }

    public Long getMaxBlockMs() {
        return maxBlockMs.toMillis();
    }

    public String getReceiveBufferBytes() {
        return receiveBufferBytes;
    }

    public Long getRequestTimeoutMs() {
        return requestTimeoutMs.toMillis();
    }

    public String getSendBufferBytes() {
        return sendBufferBytes;
    }

    public Long getSocketConnectionSetupMaxTimeout() {
        return socketConnectionSetupMaxTimeout.toMillis();
    }

    public Long getSocketConnectionSetupTimeout() {
        return socketConnectionSetupTimeout.toMillis();
    }

    public Long getMetadataMaxAgeMs() {
        return metadataMaxAgeMs.toMillis();
    }

    public Long getMetadataMaxIdleMs() {
        return metadataMaxIdleMs.toMillis();
    }

    public Long getMetricsSampleWindowMs() {
        return metricsSampleWindowMs.toMillis();
    }

    public Long getPartitionerAvailabilityTimeoutMs() {
        return partitionerAvailabilityTimeoutMs.toMillis();
    }

    public Long getReconnectBackoffMaxMs() {
        return reconnectBackoffMaxMs.toMillis();
    }

    public Long getReconnectBackoffMs() {
        return reconnectBackoffMs.toMillis();
    }

    public Long getRetryBackoffMs() {
        return retryBackoffMs.toMillis();
    }
}
