/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.plugins.sink.configuration.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.sink.configuration.ObjectOptions;
import org.opensearch.dataprepper.plugins.sink.configuration.ThresholdOptions;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;

/*
    An implementation class of s3 sink configuration
 */
public class S3SinkConfig {

    static final String DEFAULT_BUCKET_NAME = "dataprepper";
    static final String DEFAULT_PATH_PREFIX = "logdata";

    static final String DEFAULT_TEMP_STORAGE = "local_file";

    @JsonProperty("aws")
    @NotNull
    @Valid
    private AwsAuthenticationOptions awsAuthenticationOptions;

    @JsonProperty("threshold")
    @NotNull
    private ThresholdOptions thresholdOptions;

    @JsonProperty("object")
    @NotNull
    private ObjectOptions objectOptions;

   	@JsonProperty("codec")
	@NotNull
    private PluginModel codec;

    @JsonProperty("temporary_storage")
    @NotNull
    private String temporaryStorage = DEFAULT_TEMP_STORAGE;

    @JsonProperty("bucket")
    @NotNull
    private String bucketName = DEFAULT_BUCKET_NAME;

    @JsonProperty("key_path_prefix")
    @NotNull
    private String keyPathPrefix = DEFAULT_PATH_PREFIX;

    /*
        Aws Authentication configuration Options
     */
    public AwsAuthenticationOptions getAwsAuthenticationOptions() {
        return awsAuthenticationOptions;
    }

    /*
        Threshold configuration Options
     */
    public ThresholdOptions getThresholdOptions() {
        return thresholdOptions;
    }

    /*
        s3 index configuration Options
     */
    public ObjectOptions getObjectOptions() {
        return objectOptions;
    }

    /*
        sink codec configuration Options
     */
    public PluginModel getCodec() { return codec; }

    /*
        s3 index path configuration Option
     */
    public String getKeyPathPrefix() {
        return keyPathPrefix;
    }

    /*
        s3 bucket name configuration Option
     */
    public String getBucketName() {
        return bucketName;
    }

    /*
        Temporary storage location configuration Options
     */
    public String getTemporaryStorage() {
		return temporaryStorage;
	}
}
