/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNull;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.sink.accumulator.BufferTypeOptions;

class S3SinkConfigTest {

    private static final int MAX_CONNECTION_RETRIES = 5;
    private static final int MAX_UPLOAD_RETRIES = 5;

    @Test
    void default_buffer_type_option_test() {
        assertThat(new S3SinkConfig().getBufferType(), equalTo(BufferTypeOptions.INMEMORY));
    }

    @Test
    void default_max_connection_retries_test() throws NoSuchFieldException, IllegalAccessException {
        assertThat(new S3SinkConfig().getMaxConnectionRetries(), equalTo(MAX_CONNECTION_RETRIES));
    }

    @Test
    void default_max_upload_retries_test() throws NoSuchFieldException, IllegalAccessException {
        assertThat(new S3SinkConfig().getMaxUploadRetries(), equalTo(MAX_UPLOAD_RETRIES));
    }

    @Test
    void get_bucket_option_test() {
        assertThat(new S3SinkConfig().getBucketOptions(), equalTo(null));
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