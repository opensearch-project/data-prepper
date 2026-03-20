/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.sink.s3.configuration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

class ServerSideEncryptionConfigTest {

    private ServerSideEncryptionConfig createObjectUnderTest() {
        return new ServerSideEncryptionConfig();
    }

    @Test
    void defaults_to_s3_type() {
        assertThat(createObjectUnderTest().getType(), equalTo(ServerSideEncryptionType.S3));
    }

    @Test
    void defaults_kms_key_id_to_null() {
        assertThat(createObjectUnderTest().getKmsKeyId(), nullValue());
    }

    @Test
    void defaults_bucket_key_enabled_to_true() {
        assertThat(createObjectUnderTest().getBucketKeyEnabled(), equalTo(true));
    }

    @Test
    void applyTo_with_s3_type_sets_AES256() {
        final ServerSideEncryptionConfig config = createObjectUnderTest();
        final PutObjectRequest.Builder builder = PutObjectRequest.builder();

        config.applyTo(builder);

        final PutObjectRequest request = builder.bucket("test").key("test").build();
        assertThat(request.serverSideEncryption(), equalTo(ServerSideEncryption.AES256));
        assertThat(request.ssekmsKeyId(), nullValue());
        assertThat(request.bucketKeyEnabled(), nullValue());
    }

    @ParameterizedTest
    @EnumSource(value = ServerSideEncryptionType.class, names = {"KMS", "KMS_DSSE"})
    void applyTo_with_kms_type_sets_kms_key_id_and_bucket_key_enabled(final ServerSideEncryptionType type) throws Exception {
        final String kmsKeyId = UUID.randomUUID().toString();
        final ServerSideEncryptionConfig config = createObjectUnderTest();
        ReflectivelySetField.setField(ServerSideEncryptionConfig.class, config, "type", type);
        ReflectivelySetField.setField(ServerSideEncryptionConfig.class, config, "kmsKeyId", kmsKeyId);

        final PutObjectRequest.Builder builder = PutObjectRequest.builder();
        config.applyTo(builder);

        final PutObjectRequest request = builder.bucket("test").key("test").build();
        assertThat(request.serverSideEncryption(), equalTo(type.getServerSideEncryption()));
        assertThat(request.ssekmsKeyId(), equalTo(kmsKeyId));
        assertThat(request.bucketKeyEnabled(), equalTo(true));
    }

    @ParameterizedTest
    @EnumSource(value = ServerSideEncryptionType.class, names = {"KMS", "KMS_DSSE"})
    void applyTo_with_kms_type_and_no_key_id_does_not_set_kms_key_id(final ServerSideEncryptionType type) throws Exception {
        final ServerSideEncryptionConfig config = createObjectUnderTest();
        ReflectivelySetField.setField(ServerSideEncryptionConfig.class, config, "type", type);

        final PutObjectRequest.Builder builder = PutObjectRequest.builder();
        config.applyTo(builder);

        final PutObjectRequest request = builder.bucket("test").key("test").build();
        assertThat(request.serverSideEncryption(), equalTo(type.getServerSideEncryption()));
        assertThat(request.ssekmsKeyId(), nullValue());
        assertThat(request.bucketKeyEnabled(), equalTo(true));
    }

    @ParameterizedTest
    @EnumSource(value = ServerSideEncryptionType.class, names = {"KMS", "KMS_DSSE"})
    void applyTo_with_kms_type_and_bucket_key_disabled(final ServerSideEncryptionType type) throws Exception {
        final ServerSideEncryptionConfig config = createObjectUnderTest();
        ReflectivelySetField.setField(ServerSideEncryptionConfig.class, config, "type", type);
        ReflectivelySetField.setField(ServerSideEncryptionConfig.class, config, "bucketKeyEnabled", false);

        final PutObjectRequest.Builder builder = PutObjectRequest.builder();
        config.applyTo(builder);

        final PutObjectRequest request = builder.bucket("test").key("test").build();
        assertThat(request.bucketKeyEnabled(), equalTo(false));
    }

    @Test
    void applyTo_multipart_with_s3_type_sets_AES256() {
        final ServerSideEncryptionConfig config = createObjectUnderTest();
        final CreateMultipartUploadRequest.Builder builder = CreateMultipartUploadRequest.builder();

        config.applyTo(builder);

        final CreateMultipartUploadRequest request = builder.bucket("test").key("test").build();
        assertThat(request.serverSideEncryption(), equalTo(ServerSideEncryption.AES256));
        assertThat(request.ssekmsKeyId(), nullValue());
        assertThat(request.bucketKeyEnabled(), nullValue());
    }

    @ParameterizedTest
    @EnumSource(value = ServerSideEncryptionType.class, names = {"KMS", "KMS_DSSE"})
    void applyTo_multipart_with_kms_type_sets_kms_key_id_and_bucket_key_enabled(final ServerSideEncryptionType type) throws Exception {
        final String kmsKeyId = UUID.randomUUID().toString();
        final ServerSideEncryptionConfig config = createObjectUnderTest();
        ReflectivelySetField.setField(ServerSideEncryptionConfig.class, config, "type", type);
        ReflectivelySetField.setField(ServerSideEncryptionConfig.class, config, "kmsKeyId", kmsKeyId);

        final CreateMultipartUploadRequest.Builder builder = CreateMultipartUploadRequest.builder();
        config.applyTo(builder);

        final CreateMultipartUploadRequest request = builder.bucket("test").key("test").build();
        assertThat(request.serverSideEncryption(), equalTo(type.getServerSideEncryption()));
        assertThat(request.ssekmsKeyId(), equalTo(kmsKeyId));
        assertThat(request.bucketKeyEnabled(), equalTo(true));
    }

    @ParameterizedTest
    @EnumSource(value = ServerSideEncryptionType.class, names = {"KMS", "KMS_DSSE"})
    void applyTo_multipart_with_kms_type_and_no_key_id_does_not_set_kms_key_id(final ServerSideEncryptionType type) throws Exception {
        final ServerSideEncryptionConfig config = createObjectUnderTest();
        ReflectivelySetField.setField(ServerSideEncryptionConfig.class, config, "type", type);

        final CreateMultipartUploadRequest.Builder builder = CreateMultipartUploadRequest.builder();
        config.applyTo(builder);

        final CreateMultipartUploadRequest request = builder.bucket("test").key("test").build();
        assertThat(request.serverSideEncryption(), equalTo(type.getServerSideEncryption()));
        assertThat(request.ssekmsKeyId(), nullValue());
        assertThat(request.bucketKeyEnabled(), equalTo(true));
    }
}
