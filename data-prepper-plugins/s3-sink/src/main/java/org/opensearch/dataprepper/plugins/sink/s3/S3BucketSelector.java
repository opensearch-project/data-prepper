package org.opensearch.dataprepper.plugins.sink.s3;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public interface S3BucketSelector {
    /**
     *
     */
    void initialize(S3SinkConfig s3SinkConfig);

    /**
     *
     */
    String getBucketName();

    /**
     *
     */
    String getPathPrefix();

    /**
     *
     */
    Map<String, String> getMetadata(int eventCount);
}

