/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source;

import com.amazon.dataprepper.plugins.source.configuration.CompressionOption;
import com.amazon.dataprepper.plugins.source.configuration.OnErrorOption;
import org.junit.jupiter.api.Test;

import static com.amazon.dataprepper.plugins.source.S3SourceConfig.DEFAULT_NUMBER_OF_RECORDS_TO_ACCUMULATE;
import static com.amazon.dataprepper.plugins.source.S3SourceConfig.DEFAULT_REQUEST_TIMEOUT;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

class S3SourceConfigTest {

    @Test
    void default_compression_test() {
        assertThat(new S3SourceConfig().getCompression(), equalTo(CompressionOption.NONE));
    }

    @Test
    void default_on_error_test() {
        assertThat(new S3SourceConfig().getOnErrorOption(), equalTo(OnErrorOption.RETAIN_MESSAGES));
    }

    @Test
    void default_request_timeout_test() {
        assertThat(new S3SourceConfig().getRequestTimeout(), equalTo(DEFAULT_REQUEST_TIMEOUT));
    }

    @Test
    void default_records_to_accumulate_test() {
        assertThat(new S3SourceConfig().getNumberOfRecordsToAccumulate(), equalTo(DEFAULT_NUMBER_OF_RECORDS_TO_ACCUMULATE));
    }
}
