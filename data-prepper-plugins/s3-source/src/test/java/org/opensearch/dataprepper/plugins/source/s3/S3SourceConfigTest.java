/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.s3;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;
import org.opensearch.dataprepper.plugins.source.s3.configuration.NotificationSourceOption;
import org.opensearch.dataprepper.plugins.source.s3.configuration.OnErrorOption;
import org.opensearch.dataprepper.plugins.source.s3.configuration.FolderPartitioningOptions;
import org.opensearch.dataprepper.plugins.source.s3.configuration.S3ScanScanOptions;
import org.opensearch.dataprepper.plugins.source.s3.configuration.S3SelectOptions;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;

import java.util.HashMap;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.dataprepper.plugins.source.s3.S3SourceConfig.DEFAULT_BUFFER_TIMEOUT;
import static org.opensearch.dataprepper.plugins.source.s3.S3SourceConfig.DEFAULT_NUMBER_OF_RECORDS_TO_ACCUMULATE;

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
    void default_delete_s3_objects_test() {
        assertThat(new S3SourceConfig().isDeleteS3ObjectsOnRead(), equalTo(false));
    }

    @Test
    void default_notification_source_test() {
        assertThat(new S3SourceConfig().getNotificationSource(), equalTo(NotificationSourceOption.S3));
    }

    @Test
    void end_to_end_acknowledgements_set_to_true_test() throws Exception {
        final S3SourceConfig s3SourceConfig = new S3SourceConfig();
        ReflectivelySetField.setField(S3SourceConfig.class,s3SourceConfig,"acknowledgments", true);
        assertTrue(s3SourceConfig.getAcknowledgements());

        assertThat(s3SourceConfig.isPrefixPartitionModeValid(), equalTo(true));
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

    @Test
    void isPartitionModeValid_returns_false_when_using_prefix_mode_without_acknowledgments_enabled() throws NoSuchFieldException, IllegalAccessException {
        final S3SourceConfig s3SourceConfig = new S3SourceConfig();;
        final S3ScanScanOptions s3ScanScanOptions = new S3ScanScanOptions();
        assertThat(s3ScanScanOptions.getPartitioningOptions(), equalTo(null));
        final FolderPartitioningOptions folderPartitioningOptions = new FolderPartitioningOptions();
        ReflectivelySetField.setField(S3ScanScanOptions.class, s3ScanScanOptions, "folderPartitioningOptions", folderPartitioningOptions);

        ReflectivelySetField.setField(S3SourceConfig.class,s3SourceConfig,"deleteS3ObjectsOnRead", true);
        ReflectivelySetField.setField(S3SourceConfig.class,s3SourceConfig,"s3ScanScanOptions", s3ScanScanOptions);

        assertFalse(s3SourceConfig.isPrefixPartitionModeValid());
    }
}
