/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.s3.common.source;

import lombok.Getter;

import java.util.Objects;
import java.util.Optional;

/**
 * Reference to an S3 object.
 */
public class S3ObjectReference {
    @Getter
    private final String bucketName;
    @Getter
    private final String key;
    private final String owner;

    private S3ObjectReference(final String bucketName, final String key, final String owner) {
        this.bucketName = bucketName;
        this.key = key;
        this.owner = owner;
    }

    public static Builder bucketAndKey(final String bucketName, final String key) {
        Objects.requireNonNull(bucketName, "bucketName must be non null");
        Objects.requireNonNull(key, "key must be non null");
        return new Builder(bucketName, key);
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
