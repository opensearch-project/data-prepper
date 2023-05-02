/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source;

import java.io.Serializable;
import java.time.LocalDateTime;

/**
 * A S3 Object details pojo class to store the s3 object properties for Map.
 *
 */
public class S3ObjectDetails implements Serializable {
    private final String bucket;
    private final String key;
    private final LocalDateTime s3ObjectLastModifiedTimestamp;

    public S3ObjectDetails(final String bucket,final String key,final LocalDateTime s3ObjectLastModifiedTimestamp) {
        this.bucket = bucket;
        this.key = key;
        this.s3ObjectLastModifiedTimestamp = s3ObjectLastModifiedTimestamp;
    }

   public String getBucket() {
        return bucket;
    }

    public String getKey() {
        return key;
    }

    public LocalDateTime getS3ObjectLastModifiedTimestamp() {
        return s3ObjectLastModifiedTimestamp;
    }
}