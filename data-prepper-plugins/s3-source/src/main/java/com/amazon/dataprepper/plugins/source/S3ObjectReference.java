/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source;

import java.util.Objects;

/**
 * Reference to an S3 object.
 */
class S3ObjectReference {
    private final String bucketName;
    private final String key;

    private S3ObjectReference(final String bucketName, final String key) {
        Objects.requireNonNull(bucketName, "bucketName must be non null");
        Objects.requireNonNull(key, "key must be non null");

        this.bucketName = bucketName;
        this.key = key;
    }

    static S3ObjectReference fromBucketAndKey(final String bucketName, final String key) {
        return new S3ObjectReference(bucketName, key);
    }

    String getBucketName() {
        return bucketName;
    }

    String getKey() {
        return key;
    }

    @Override
    public String toString() {
        return "[bucketName=" + bucketName + ", key=" + key + "]";
    }
}
