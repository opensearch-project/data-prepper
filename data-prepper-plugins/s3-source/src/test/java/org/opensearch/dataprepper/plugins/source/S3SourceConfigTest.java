/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source;

import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.plugins.source.configuration.CompressionOption;
import org.opensearch.dataprepper.plugins.source.configuration.OnErrorOption;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.source.configuration.S3SelectOptions;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;

import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.dataprepper.plugins.source.S3SourceConfig.DEFAULT_NUMBER_OF_RECORDS_TO_ACCUMULATE;
import static org.opensearch.dataprepper.plugins.source.S3SourceConfig.DEFAULT_BUFFER_TIMEOUT;
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
        assertThat(new S3SourceConfig().getBufferTimeout(), equalTo(DEFAULT_BUFFER_TIMEOUT));
    }

    @Test
    void default_end_to_end_acknowledgements_test() {
        assertThat(new S3SourceConfig().getAcknowledgements(), equalTo(false));
    }

    @Test
    void end_to_end_acknowledgements_set_to_true_test() throws Exception {
        final S3SourceConfig s3SourceConfig = new S3SourceConfig();
        ReflectivelySetField.setField(S3SourceConfig.class,s3SourceConfig,"acknowledgements", true);
        assertTrue(s3SourceConfig.getAcknowledgements());
    }

    @Test
    void default_records_to_accumulate_test() {
        assertThat(new S3SourceConfig().getNumberOfRecordsToAccumulate(), equalTo(DEFAULT_NUMBER_OF_RECORDS_TO_ACCUMULATE));
    }

    @Test
    void isCodecProvidedWhenNeeded_returns_true_when_s3SelectOptions_is_non_null_and_codec_is_null() throws Exception{
        final S3SourceConfig s3SourceConfig = new S3SourceConfig();
        ReflectivelySetField.setField(S3SourceConfig.class,s3SourceConfig,"s3SelectOptions",new S3SelectOptions());
        assertTrue(s3SourceConfig.isCodecProvidedWhenNeeded());
    }

    @Test
    void isCodecProvidedWhenNeeded_returns_true_when_s3SelectOptions_is_null_and_codec_is_non_null() throws Exception {
        final S3SourceConfig s3SourceConfig = new S3SourceConfig();
        ReflectivelySetField.setField(S3SourceConfig.class,s3SourceConfig,"codec",new PluginModel("csv",new HashMap<>()));
        assertTrue(s3SourceConfig.isCodecProvidedWhenNeeded());
    }

    @Test
    void isCodecProvidedWhenNeeded_returns_false_when_s3SelectOptions_is_null_and_codec_is_null(){
        final S3SourceConfig s3SourceConfig = new S3SourceConfig();
        assertFalse(s3SourceConfig.isCodecProvidedWhenNeeded());
    }
}
