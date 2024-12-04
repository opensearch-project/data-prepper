package org.opensearch.dataprepper.plugins.source.neptune.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.datatype.jsr310.deser.DurationDeserializer;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;

import java.time.Duration;

@Getter
@Setter
public class NeptuneSourceConfig {
    private static final int DEFAULT_PORT = 8182;
    private static final Duration DEFAULT_ACKNOWLEDGEMENT_SET_TIMEOUT = Duration.ofHours(2);

    @JsonProperty("host")
    private @NotNull String host;
    @JsonProperty("port")
    private Integer port = DEFAULT_PORT;
    @JsonProperty("region")
    private String region;
    @JsonProperty("iam_auth")
    private boolean iamAuth;

    @JsonProperty("trust_store_file_path")
    private String trustStoreFilePath;
    @JsonProperty("trust_store_password")
    private String trustStorePassword;

    @JsonProperty("s3_bucket")
    private String s3Bucket;
    @JsonProperty("s3_prefix")
    private String s3Prefix;
    @JsonProperty("s3_region")
    private String s3Region;

    @JsonProperty("enable_non_string_indexing")
    private boolean enableNonStringIndexing = false;

    @JsonProperty("acknowledgments")
    private boolean acknowledgments = false;
    @JsonProperty("partition_acknowledgment_timeout")
    @JsonDeserialize(using = DurationDeserializer.class)
    private Duration partitionAcknowledgmentTimeout = DEFAULT_ACKNOWLEDGEMENT_SET_TIMEOUT;

    @JsonProperty("aws")
    @NotNull
    @Valid
    private AwsConfig awsConfig;

    @JsonProperty("export")
    private boolean export = false;
    @JsonProperty("stream")
    private boolean stream = false;
    @JsonProperty("stream_type")
    private String streamType;
}
