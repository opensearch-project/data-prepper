/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.source.configuration.CompressionOption;
import org.opensearch.dataprepper.plugins.source.configuration.S3ScanBucketOption;
import org.opensearch.dataprepper.plugins.source.configuration.S3ScanBucketOptions;
import org.opensearch.dataprepper.plugins.source.configuration.S3ScanKeyPathOption;
import org.opensearch.dataprepper.plugins.source.configuration.S3ScanScanOptions;
import org.opensearch.dataprepper.plugins.source.configuration.S3SelectCSVOption;
import org.opensearch.dataprepper.plugins.source.configuration.S3SelectOptions;
import org.opensearch.dataprepper.plugins.source.configuration.S3SelectSerializationFormatOption;


import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class S3ScanServiceTest {

    @Test
    void scan_service_test_and_verify_thread_invoking() {
        S3ScanService s3ScanService = mock(S3ScanService.class);
        s3ScanService.start();
        verify(s3ScanService,times(1)).start();
    }

    @Test
    public void scan_service_with_s3_select_Configuration_test_and_verify() {
        String bucketName="my-bucket-5";
        String startDate = "2023-03-07T10:00:00";
        String range = "P2DT1H";
        String expression = "select * from s3Object";
        List<String> includeKeyPathList = List.of("file1.csv","file2.csv");
        S3SelectSerializationFormatOption serializationFormatOption = S3SelectSerializationFormatOption.CSV;
        S3SourceConfig s3SourceConfig = mock(S3SourceConfig.class);
        S3ScanScanOptions s3ScanScanOptions = mock(S3ScanScanOptions.class);
        when(s3ScanScanOptions.getStartTime()).thenReturn(LocalDateTime.parse(startDate));
        when(s3ScanScanOptions.getRange()).thenReturn(Duration.parse(range));
        S3ScanBucketOptions bucket = mock(S3ScanBucketOptions.class);
        final S3ScanBucketOption s3SelectOptions = mock(S3ScanBucketOption.class);
        final S3SelectOptions s3Select = mock(S3SelectOptions.class);
        final S3SelectCSVOption csvOption = mock(S3SelectCSVOption.class);
        when(s3Select.getExpressionType()).thenReturn("SQL");
        when(s3Select.getExpression()).thenReturn(expression);
        when(s3Select.getS3SelectSerializationFormatOption()).thenReturn(serializationFormatOption);
        when(s3Select.getS3SelectCSVOption()).thenReturn(csvOption);
        when(s3Select.getCompressionType()).thenReturn("none");
        when(s3SelectOptions.getName()).thenReturn(bucketName);
        S3ScanKeyPathOption s3ScanKeyPathOption = mock(S3ScanKeyPathOption.class);
        when(s3ScanKeyPathOption.getS3scanIncludeOptions()).thenReturn(includeKeyPathList);
        when(s3SelectOptions.getKeyPath()).thenReturn(s3ScanKeyPathOption);
        when(s3SelectOptions.getCompression()).thenReturn(CompressionOption.NONE);
        when(bucket.getS3ScanBucketOption()).thenReturn(s3SelectOptions);
        List<S3ScanBucketOptions> buckets = new ArrayList<>();
        buckets.add(bucket);
        when(s3ScanScanOptions.getBuckets()).thenReturn(buckets);
        when(s3SourceConfig.getS3ScanScanOptions()).thenReturn(s3ScanScanOptions);
        S3ScanService service = new S3ScanService(s3SourceConfig,mock(S3ClientBuilderFactory.class),mock(S3ObjectHandler.class));
        final List<ScanOptions> scanOptionsBuilder = service.getScanOptions();
        assertThat(scanOptionsBuilder.get(0).getIncludeKeyPaths(),sameInstance(includeKeyPathList));
        assertThat(scanOptionsBuilder.get(0).getBucket(),sameInstance(bucketName));
        assertThat(scanOptionsBuilder.get(0).getRange(),equalTo(Duration.parse(range)));
        assertThat(scanOptionsBuilder.get(0).getStartDateTime(),equalTo(LocalDateTime.parse(startDate)));
    }
}
