/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.s3;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.source.coordinator.SourceCoordinator;
import org.opensearch.dataprepper.plugins.source.s3.configuration.S3ScanBucketOption;
import org.opensearch.dataprepper.plugins.source.s3.configuration.S3ScanBucketOptions;
import org.opensearch.dataprepper.plugins.source.s3.configuration.S3ScanKeyPathOption;
import org.opensearch.dataprepper.plugins.source.s3.configuration.S3ScanScanOptions;
import org.opensearch.dataprepper.plugins.source.s3.ownership.BucketOwnerProvider;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;

import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class S3ScanServiceTest {
    @Mock
    private S3ObjectDeleteWorker s3ObjectDeleteWorker;
    @Mock
    private SourceCoordinator sourceCoordinator;
    @Mock
    private BucketOwnerProvider bucketOwnerProvider;
    @Mock
    private S3ClientBuilderFactory s3ClientBuilderFactory;
    @Mock
    private S3ObjectHandler s3ObjectHandler;
    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;
    @Mock
    private PluginMetrics pluginMetrics;

    @Test
    void scan_service_test_and_verify_thread_invoking() {
        S3ScanService s3ScanService = mock(S3ScanService.class);
        s3ScanService.start();
        verify(s3ScanService,times(1)).start();
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 5})
    void scan_service_with_valid_s3_scan_configuration_test_and_verify(int numWorkers) {
        final String bucketName="my-bucket-5";
        final LocalDateTime startDateTime = LocalDateTime.parse("2023-03-07T10:00:00");
        final List<String> includeKeyPathList = List.of("file1.csv","file2.csv");
        final S3SourceConfig s3SourceConfig = mock(S3SourceConfig.class);
        final S3ScanScanOptions s3ScanScanOptions = mock(S3ScanScanOptions.class);
        when(s3SourceConfig.getNumWorkers()).thenReturn(numWorkers);
        when(s3ScanScanOptions.getStartTime()).thenReturn(startDateTime);
        S3ScanBucketOptions bucket = mock(S3ScanBucketOptions.class);
        final S3ScanBucketOption s3ScanBucketOption = mock(S3ScanBucketOption.class);
        when(s3ScanBucketOption.getName()).thenReturn(bucketName);
        S3ScanKeyPathOption s3ScanKeyPathOption = mock(S3ScanKeyPathOption.class);
        when(s3ScanKeyPathOption.getS3scanIncludePrefixOptions()).thenReturn(includeKeyPathList);
        when(s3ScanBucketOption.getRange()).thenReturn(null);
        when(s3ScanBucketOption.getS3ScanFilter()).thenReturn(s3ScanKeyPathOption);
        when(bucket.getS3ScanBucketOption()).thenReturn(s3ScanBucketOption);
        when(s3ScanScanOptions.getBuckets()).thenReturn(List.of(bucket));
        when(s3SourceConfig.getS3ScanScanOptions()).thenReturn(s3ScanScanOptions);
        S3ScanService service = new S3ScanService(s3SourceConfig, s3ClientBuilderFactory, s3ObjectHandler, bucketOwnerProvider, sourceCoordinator, acknowledgementSetManager, s3ObjectDeleteWorker, pluginMetrics);
        final List<ScanOptions> scanOptionsBuilder = service.getScanOptions();
        assertThat(scanOptionsBuilder.get(0).getBucketOption().getS3ScanFilter().getS3scanIncludePrefixOptions(),sameInstance(includeKeyPathList));
        assertThat(scanOptionsBuilder.get(0).getBucketOption().getName(),sameInstance(bucketName));
        assertThat(scanOptionsBuilder.get(0).getUseStartDateTime(),equalTo(startDateTime));
        assertThat(scanOptionsBuilder.get(0).getUseEndDateTime(),equalTo(null));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 5})
    void scan_service_with_valid_bucket_time_range_configuration_test_and_verify(int numWorkers) {
        final String bucketName="my-bucket-5";
        final Duration range = Duration.parse("P2DT1H");
        final List<String> includeKeyPathList = List.of("file1.csv","file2.csv");
        final S3SourceConfig s3SourceConfig = mock(S3SourceConfig.class);
        final S3ScanScanOptions s3ScanScanOptions = mock(S3ScanScanOptions.class);
        when(s3SourceConfig.getNumWorkers()).thenReturn(numWorkers);
        S3ScanBucketOptions bucket = mock(S3ScanBucketOptions.class);
        final S3ScanBucketOption s3ScanBucketOption = mock(S3ScanBucketOption.class);
        when(s3ScanBucketOption.getName()).thenReturn(bucketName);
        S3ScanKeyPathOption s3ScanKeyPathOption = mock(S3ScanKeyPathOption.class);
        when(s3ScanKeyPathOption.getS3scanIncludePrefixOptions()).thenReturn(includeKeyPathList);
        when(s3ScanBucketOption.getRange()).thenReturn(range);
        when(s3ScanBucketOption.getS3ScanFilter()).thenReturn(s3ScanKeyPathOption);
        when(bucket.getS3ScanBucketOption()).thenReturn(s3ScanBucketOption);
        when(s3ScanScanOptions.getBuckets()).thenReturn(List.of(bucket));
        when(s3SourceConfig.getS3ScanScanOptions()).thenReturn(s3ScanScanOptions);
        S3ScanService service = new S3ScanService(s3SourceConfig, s3ClientBuilderFactory, s3ObjectHandler, bucketOwnerProvider, sourceCoordinator, acknowledgementSetManager, s3ObjectDeleteWorker, pluginMetrics);
        final List<ScanOptions> scanOptionsBuilder = service.getScanOptions();
        assertThat(scanOptionsBuilder.get(0).getBucketOption().getS3ScanFilter().getS3scanIncludePrefixOptions(),sameInstance(includeKeyPathList));
        assertThat(scanOptionsBuilder.get(0).getBucketOption().getName(),sameInstance(bucketName));
        assertThat(scanOptionsBuilder.get(0).getUseStartDateTime(), lessThanOrEqualTo(LocalDateTime.now().minus(range)));
        assertThat(scanOptionsBuilder.get(0).getUseStartDateTime(), greaterThanOrEqualTo(LocalDateTime.now().minus(range).minus(Duration.parse("PT5S"))));
        assertThat(scanOptionsBuilder.get(0).getUseEndDateTime(), lessThanOrEqualTo(LocalDateTime.now()));
        assertThat(scanOptionsBuilder.get(0).getUseEndDateTime(), greaterThanOrEqualTo(LocalDateTime.now().minus(Duration.parse("PT5S"))));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 5})
    void scan_service_with_no_time_range_configuration_test_and_verify(int numWorkers) {
        final String bucketName="my-bucket-5";
        final List<String> includeKeyPathList = List.of("file1.csv","file2.csv");
        final S3SourceConfig s3SourceConfig = mock(S3SourceConfig.class);
        final S3ScanScanOptions s3ScanScanOptions = mock(S3ScanScanOptions.class);
        when(s3ScanScanOptions.getRange()).thenReturn(null);
        when(s3SourceConfig.getNumWorkers()).thenReturn(numWorkers);
        S3ScanBucketOptions bucket = mock(S3ScanBucketOptions.class);
        final S3ScanBucketOption s3ScanBucketOption = mock(S3ScanBucketOption.class);
        when(s3ScanBucketOption.getName()).thenReturn(bucketName);
        when(s3ScanBucketOption.getRange()).thenReturn(null);
        S3ScanKeyPathOption s3ScanKeyPathOption = mock(S3ScanKeyPathOption.class);
        when(s3ScanKeyPathOption.getS3scanIncludePrefixOptions()).thenReturn(includeKeyPathList);
        when(s3ScanBucketOption.getS3ScanFilter()).thenReturn(s3ScanKeyPathOption);
        when(bucket.getS3ScanBucketOption()).thenReturn(s3ScanBucketOption);
        when(s3ScanScanOptions.getBuckets()).thenReturn(List.of(bucket));
        when(s3SourceConfig.getS3ScanScanOptions()).thenReturn(s3ScanScanOptions);
        S3ScanService service = new S3ScanService(s3SourceConfig, s3ClientBuilderFactory, s3ObjectHandler, bucketOwnerProvider, sourceCoordinator, acknowledgementSetManager, s3ObjectDeleteWorker, pluginMetrics);
        final List<ScanOptions> scanOptionsBuilder = service.getScanOptions();
        assertThat(scanOptionsBuilder.get(0).getBucketOption().getS3ScanFilter().getS3scanIncludePrefixOptions(),sameInstance(includeKeyPathList));
        assertThat(scanOptionsBuilder.get(0).getBucketOption().getName(),sameInstance(bucketName));
        assertThat(scanOptionsBuilder.get(0).getUseStartDateTime(),equalTo(null));
        assertThat(scanOptionsBuilder.get(0).getUseEndDateTime(),equalTo(null));
    }
}
