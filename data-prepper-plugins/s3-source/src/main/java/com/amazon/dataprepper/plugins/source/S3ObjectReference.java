/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source;

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

    @Override
    public String toString() {
        return "[bucketName=" + bucketName + ", key=" + key + "]";
    }

    public Optional<String> getBucketOwner() {
        return Optional.ofNullable(owner);
    }

    public static final class Builder {

        private final String bucketName;
        private final String key;
        private String owner;

        public Builder(final String bucketName, final String key) {
            this.bucketName = bucketName;
            this.key = key;
        }

        Builder owner(final String owner) {
            this.owner = owner;
            return this;
        }

        S3ObjectReference build() {
            return new S3ObjectReference(bucketName, key, owner);
        }
    }
}
