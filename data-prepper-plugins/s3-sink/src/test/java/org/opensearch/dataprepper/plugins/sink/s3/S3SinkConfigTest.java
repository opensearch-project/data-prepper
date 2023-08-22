/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.sink.s3.accumulator.BufferTypeOptions;
import org.opensearch.dataprepper.plugins.sink.s3.configuration.ObjectKeyOptions;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.opensearch.dataprepper.plugins.sink.s3.S3SinkConfig.S3_PREFIX;

class S3SinkConfigTest {

    private static final int MAX_CONNECTION_RETRIES = 5;
    private static final int MAX_UPLOAD_RETRIES = 5;

    @Test
    void default_buffer_type_option_test() {
        assertThat(new S3SinkConfig().getBufferType(), equalTo(BufferTypeOptions.INMEMORY));
    }

    @Test
    void default_max_connection_retries_test() {
        assertThat(new S3SinkConfig().getMaxConnectionRetries(), equalTo(MAX_CONNECTION_RETRIES));
    }

    @Test
    void default_max_upload_retries_test() {
        assertThat(new S3SinkConfig().getMaxUploadRetries(), equalTo(MAX_UPLOAD_RETRIES));
    }

    @Test
    void get_bucket_name_test() throws NoSuchFieldException, IllegalAccessException {
        final String bucketName = UUID.randomUUID().toString();
        final S3SinkConfig objectUnderTest = new S3SinkConfig();
        ReflectivelySetField.setField(S3SinkConfig.class, objectUnderTest, "bucketName", bucketName);
        assertThat(objectUnderTest.getBucketName(), equalTo(bucketName));
    }

    @Test
    void get_bucket_name_with_s3_prefix_test() throws NoSuchFieldException, IllegalAccessException {
        final String bucketName = UUID.randomUUID().toString();
        final String bucketNameWithPrefix = S3_PREFIX + bucketName;
        final S3SinkConfig objectUnderTest = new S3SinkConfig();
        ReflectivelySetField.setField(S3SinkConfig.class, objectUnderTest, "bucketName", bucketNameWithPrefix);
        assertThat(objectUnderTest.getBucketName(), equalTo(bucketName));
    }

    @Test
    void get_object_key_test() {
        assertThat("Object key is not an instance of ObjectKeyOptions",
                new S3SinkConfig().getObjectKeyOptions(), instanceOf(ObjectKeyOptions.class));
    }


    @Test
    void get_threshold_option_test() {
        assertThat(new S3SinkConfig().getThresholdOptions(), equalTo(null));
    }

    @Test
    void get_AWS_Auth_options_in_sinkconfig_exception() {
        assertNull(new S3SinkConfig().getAwsAuthenticationOptions());
    }

    @Test
    void get_json_codec_test() {
        assertNull(new S3SinkConfig().getCodec());
    }
}