/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class S3ObjectReferenceTest {

    private String bucketName;
    private String key;

    @BeforeEach
    void setUp() {
        bucketName = UUID.randomUUID().toString();
        key = UUID.randomUUID().toString();
    }

    @Test
    void fromBucketAndKey_throws_with_null_bucketName() {
        assertThrows(NullPointerException.class, () -> S3ObjectReference.fromBucketAndKey(null, key));
    }

    @Test
    void fromBucketAndKey_throws_with_null_key() {
        assertThrows(NullPointerException.class, () -> S3ObjectReference.fromBucketAndKey(bucketName, null));
    }

    @Test
    void fromBucketAndKey_creates_object_with_correct_values() {
        final S3ObjectReference objectUnderTest = S3ObjectReference.fromBucketAndKey(bucketName, key);

        assertThat(objectUnderTest.getBucketName(), equalTo(bucketName));
        assertThat(objectUnderTest.getKey(), equalTo(key));
    }

    @Test
    void toString_returns_string_with_bucket_and_key() {
        final S3ObjectReference objectUnderTest = S3ObjectReference.fromBucketAndKey(bucketName, key);

        assertThat(objectUnderTest.toString(), containsString(bucketName));
        assertThat(objectUnderTest.toString(), containsString(key));
    }
}