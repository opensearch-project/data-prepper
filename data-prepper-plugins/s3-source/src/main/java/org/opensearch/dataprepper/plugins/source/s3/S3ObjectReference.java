/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.s3;

import java.util.Objects;
import java.util.Optional;

/**
 * Reference to an S3 object.
 */
class S3ObjectReference {
    private final String bucketName;
    private final String key;
    private final String owner;

    private S3ObjectReference(final String bucketName, final String key, final String owner) {
        this.bucketName = bucketName;
        this.key = key;
        this.owner = owner;
    }

    static Builder bucketAndKey(final String bucketName, final String key) {
        Objects.requireNonNull(bucketName, "bucketName must be non null");
        Objects.requireNonNull(key, "key must be non null");
        return new Builder(bucketName, key);
    }

    String getBucketName() {
        return bucketName;
    }

    String getKey() {
        return key;
    }

    Optional<String> getBucketOwner() {
        return Optional.ofNullable(owner);
    }

    @Override
    public String toString() {
        return "[bucketName=" + bucketName + ", key=" + key + "]";
    }

    public String uri() {
        return String.format("s3://%s/%s", bucketName, key);
    }

    public static final class Builder {
        private final String bucketName;
        private final String key;
        private String owner;

        private Builder(final String bucketName, final String key) {
            this.bucketName = bucketName;
            this.key = key;
        }

        public Builder owner(final String owner) {
            this.owner = owner;
            return this;
        }

        public S3ObjectReference build() {
            return new S3ObjectReference(bucketName, key, owner);
        }
    }
}
