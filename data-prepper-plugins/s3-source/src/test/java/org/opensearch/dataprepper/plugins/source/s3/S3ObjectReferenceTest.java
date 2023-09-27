/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.s3;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
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
    void bucketAndKey_throws_with_null_bucketName() {
        assertThrows(NullPointerException.class, () -> S3ObjectReference.bucketAndKey(null, key));
    }

    @Test
    void bucketAndKey_throws_with_null_key() {
        assertThrows(NullPointerException.class, () -> S3ObjectReference.bucketAndKey(bucketName, null));
    }

    @Test
    void build_creates_object_with_correct_values() {
        final S3ObjectReference objectUnderTest = S3ObjectReference.bucketAndKey(bucketName, key).build();

        assertThat(objectUnderTest.getBucketName(), equalTo(bucketName));
        assertThat(objectUnderTest.getKey(), equalTo(key));
        assertThat(objectUnderTest.getBucketOwner(), notNullValue());
        assertThat(objectUnderTest.getBucketOwner().isPresent(), equalTo(false));
    }

    @Test
    void build_creates_object_with_correct_values_when_owner_is_provided() {
        final String owner = UUID.randomUUID().toString();
        final S3ObjectReference objectUnderTest = S3ObjectReference
                .bucketAndKey(bucketName, key)
                .owner(owner)
                .build();

        assertThat(objectUnderTest.getBucketName(), equalTo(bucketName));
        assertThat(objectUnderTest.getKey(), equalTo(key));
        assertThat(objectUnderTest.getBucketOwner(), notNullValue());
        assertThat(objectUnderTest.getBucketOwner().isPresent(), equalTo(true));
        assertThat(objectUnderTest.getBucketOwner().get(), equalTo(owner));
    }

    @Test
    void toString_returns_string_with_bucket_and_key() {
        final S3ObjectReference objectUnderTest = S3ObjectReference.bucketAndKey(bucketName, key).build();

        assertThat(objectUnderTest.toString(), containsString(bucketName));
        assertThat(objectUnderTest.toString(), containsString(key));
    }
}