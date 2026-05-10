/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.dlq.s3;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class S3DlqWriterConfigTest {

    @Test
    public void testDefaultRegion() {
        assertThat(new S3DlqWriterConfig().getRegion(), is(equalTo(Region.US_EAST_1)));
    }

    @Test
    public void testDefaultKeyPathPrefix() {
        assertThat(new S3DlqWriterConfig().getKeyPathPrefix(), is(equalTo(null)));
    }

    @Test
    public void testDefaultForcePathStyle() {
        assertThat(new S3DlqWriterConfig().getForcePathStyle(), is(equalTo(false)));
    }

    @ParameterizedTest
    @ValueSource(strings = {"foobar", "arn:aws:es:us-west-2:123456789012:domain/bogus-domain",
        "arn:aws:iam::123456789012:group/bogus-group"})
    public void getS3ClientWithInvalidStsRoleArnThrowException(final String stsRoleArn) throws NoSuchFieldException, IllegalAccessException {
        final S3DlqWriterConfig config = new S3DlqWriterConfig();
        reflectivelySetField(config, "stsRoleArn", stsRoleArn);
        assertThrows(IllegalArgumentException.class, config::getS3Client);
    }

    @ParameterizedTest
    @CsvSource({"bucket-name, bucket-name", "s3://bucket-name, bucket-name"})
    public void getS3BucketNameShouldReturnCorrectBucketName(final String bucketName, final String expectedBucketName) throws NoSuchFieldException, IllegalAccessException {
        final S3DlqWriterConfig config = new S3DlqWriterConfig();
        reflectivelySetField(config, "bucket", bucketName);
        assertThat(config.getBucket(), is(equalTo(expectedBucketName)));
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {"", "arn:aws:iam::123456789012:role/some-role"})
    public void getS3ClientWithValidStsRoleArn(final String stsRoleArn) throws NoSuchFieldException, IllegalAccessException {
        final S3DlqWriterConfig config = new S3DlqWriterConfig();
        reflectivelySetField(config, "stsRoleArn", stsRoleArn);
        final S3Client s3Client = config.getS3Client();
        assertThat(s3Client, is(notNullValue()));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void getS3ClientWithValidAccessStyle(final boolean forcePathStyle) throws NoSuchFieldException, IllegalAccessException {
        final S3DlqWriterConfig config = new S3DlqWriterConfig();
        reflectivelySetField(config, "forcePathStyle", forcePathStyle);
        final S3Client s3Client = config.getS3Client();
        assertThat(s3Client, is(notNullValue()));
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {"", "arn:aws:iam::123456789012:role/some-role"})
    public void getS3ClientWithValidStsRoleArnAndExternalId(final String stsRoleArn) throws NoSuchFieldException, IllegalAccessException {
        final S3DlqWriterConfig config = new S3DlqWriterConfig();
        reflectivelySetField(config, "stsRoleArn", stsRoleArn);
        reflectivelySetField(config, "stsExternalId", UUID.randomUUID().toString());
        reflectivelySetField(config, "stsHeaderOverrides", Map.of(UUID.randomUUID().toString(), UUID.randomUUID().toString()));
        final S3Client s3Client = config.getS3Client();
        assertThat(s3Client, is(notNullValue()));
    }

    private void reflectivelySetField(final S3DlqWriterConfig config, final String fieldName, final Object value) throws NoSuchFieldException, IllegalAccessException {
        final Field field = S3DlqWriterConfig.class.getDeclaredField(fieldName);
        try {
            field.setAccessible(true);
            field.set(config, value);
        } finally {
            field.setAccessible(false);
        }
    }
}
