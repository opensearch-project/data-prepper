/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3;

import java.util.Map;

public interface S3BucketSelector {
    /**
     * initialize - initializes the selector
     * @param s3SinkConfig - s3 sink configuration
     */
    void initialize(S3SinkConfig s3SinkConfig);

    /**
     * getBucketName - returns the name of the bucket created by the bucket selector
     *
     * @return - bucket name
     */
    String getBucketName();

    /**
     * getPathPrefix - returns the prefix to be used for the objects created in the bucket
     *
     * @return path prefix
     */
    String getPathPrefix();

    /**
     * getMetadata - returns metadata to be used when creating the objects
     * @param eventCount - count of number of events
     *
     * @return - returns a map of metadata
     */
    Map<String, String> getMetadata(int eventCount);
}

