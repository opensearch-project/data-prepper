/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source;

import org.junit.Test;
import org.mockito.Mock;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.configuration.CompressionOption;
import org.opensearch.dataprepper.plugins.source.configuration.S3ScanBucketOption;
import org.opensearch.dataprepper.plugins.source.configuration.S3ScanBucketOptions;
import org.opensearch.dataprepper.plugins.source.configuration.S3ScanScanOptions;
import org.opensearch.dataprepper.plugins.source.configuration.S3SelectOptions;
import org.opensearch.dataprepper.plugins.source.configuration.S3SelectSerializationFormatOption;
import org.opensearch.dataprepper.plugins.source.ownership.BucketOwnerProvider;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class S3ScanServiceTest {
    @Mock
    private S3SourceConfig s3SourceConfig;
    @Mock
    private Buffer<Record<Event>> buffer;
    @Mock
    private BucketOwnerProvider bucketOwnerProvider;
    @Mock
    private BiConsumer<Event, S3ObjectReference> eventConsumer;
    @Mock
    private PluginFactory pluginFactory;
    @Mock
    private S3ClientBuilderFactory s3ClientBuilderFactory;
    @Mock
    private S3Client s3Client;
    @Mock
    private S3AsyncClient s3AsyncClient;
    @Mock
    private S3ObjectPluginMetrics s3ObjectPluginMetrics;
    @Test
    public void testS3ScanServiceWithS3ObjectReferenceConfiguration() {
        String bucketName="my-bucket-1";
        String key = "file1.csv";
        final List<String> keyList = Arrays.asList(key);
        s3SourceConfig = mock(S3SourceConfig.class);
        S3ScanScanOptions s3ScanScanOptions = mock(S3ScanScanOptions.class);
        when(s3ScanScanOptions.getStartTime()).thenReturn("2023-03-07T10:00:00");
        when(s3ScanScanOptions.getRange()).thenReturn("2d");
        S3ScanBucketOptions bucket = mock(S3ScanBucketOptions.class);
        final S3ScanBucketOption s3SelectOptions = mock(S3ScanBucketOption.class);
        when(s3SelectOptions.getName()).thenReturn(bucketName);
        when(s3SelectOptions.getKeyPath()).thenReturn(keyList);
        when(s3SelectOptions.getCompression()).thenReturn(CompressionOption.NONE);
        when(s3SelectOptions.getCodec()).thenReturn(mock(PluginModel.class));
        when(bucket.getBucket()).thenReturn(s3SelectOptions);
        List<S3ScanBucketOptions> buckets = new ArrayList<>();
        buckets.add(bucket);
        when(s3ScanScanOptions.getBuckets()).thenReturn(buckets);
        when(s3SourceConfig.getS3ScanScanOptions()).thenReturn(s3ScanScanOptions);
        s3ClientBuilderFactory = mock(S3ClientBuilderFactory.class);
        when(s3ClientBuilderFactory.getS3Client()).thenReturn(s3Client);
        when(s3ClientBuilderFactory.getS3AsyncClient()).thenReturn(s3AsyncClient);
        pluginFactory = mock(PluginFactory.class);
        bucketOwnerProvider = mock(BucketOwnerProvider.class);
        when(bucketOwnerProvider.getBucketOwner(bucketName)).thenReturn(Optional.of(bucketName));
        S3ScanService service = new S3ScanService(s3SourceConfig,buffer,bucketOwnerProvider,eventConsumer,s3ObjectPluginMetrics,pluginFactory,s3ClientBuilderFactory);
        service.start();
        final List<ScanOptionsBuilder> scanOptionsBuilder = service.getScanOptions();
        assertThat(scanOptionsBuilder,notNullValue());
        assertThat(scanOptionsBuilder.get(0).getQuery(),nullValue());
        assertThat(scanOptionsBuilder.get(0).getKeys(),sameInstance(keyList));
    }

    @Test
    public void testS3ScanServiceWithS3SelectConfiguration() throws IOException {
        String bucketName="my-bucket-1";
        String key = "file1.csv";
        final List<String> keyList = Arrays.asList(key);
        String queryStatement = "select * from s3Object";
        S3SelectSerializationFormatOption serializationFormatOption = S3SelectSerializationFormatOption.CSV;
        s3SourceConfig = mock(S3SourceConfig.class);
        S3ScanScanOptions s3ScanScanOptions = mock(S3ScanScanOptions.class);
        when(s3ScanScanOptions.getStartTime()).thenReturn("2023-03-07T10:00:00");
        when(s3ScanScanOptions.getRange()).thenReturn("2d");
        S3ScanBucketOptions bucket = mock(S3ScanBucketOptions.class);
        final S3ScanBucketOption s3SelectOptions = mock(S3ScanBucketOption.class);
        final S3SelectOptions s3Select = mock(S3SelectOptions.class);
        when(s3Select.getQueryStatement()).thenReturn(queryStatement);
        when(s3Select.getS3SelectSerializationFormatOption()).thenReturn(serializationFormatOption);
        when(s3Select.getCsvFileHeaderInfo()).thenReturn("NONE");
        when(s3SelectOptions.getName()).thenReturn(bucketName);
        when(s3SelectOptions.getS3SelectOptions()).thenReturn(s3Select);
        when(s3SelectOptions.getKeyPath()).thenReturn(keyList);
        when(s3SelectOptions.getCompression()).thenReturn(CompressionOption.NONE);
        when(s3SelectOptions.getCodec()).thenReturn(mock(PluginModel.class));
        when(bucket.getBucket()).thenReturn(s3SelectOptions);
        List<S3ScanBucketOptions> buckets = new ArrayList<>();
        buckets.add(bucket);
        when(s3ScanScanOptions.getBuckets()).thenReturn(buckets);
        when(s3SourceConfig.getS3ScanScanOptions()).thenReturn(s3ScanScanOptions);
        s3ClientBuilderFactory = mock(S3ClientBuilderFactory.class);
        when(s3ClientBuilderFactory.getS3Client()).thenReturn(s3Client);
        when(s3ClientBuilderFactory.getS3AsyncClient()).thenReturn(s3AsyncClient);
        pluginFactory = mock(PluginFactory.class);
        bucketOwnerProvider = mock(BucketOwnerProvider.class);
        when(bucketOwnerProvider.getBucketOwner(bucketName)).thenReturn(Optional.of(bucketName));
        S3ScanService service = new S3ScanService(s3SourceConfig,buffer,bucketOwnerProvider,eventConsumer,s3ObjectPluginMetrics,pluginFactory,s3ClientBuilderFactory);
        service.start();
        final List<ScanOptionsBuilder> scanOptionsBuilder = service.getScanOptions();
        assertThat(scanOptionsBuilder,notNullValue());
        assertThat(scanOptionsBuilder.get(0).getQuery(),sameInstance(queryStatement));
        assertThat(scanOptionsBuilder.get(0).getBucket(),sameInstance(bucketName));
        assertThat(scanOptionsBuilder.get(0).getKeys(),sameInstance(keyList));
        assertThat(scanOptionsBuilder.get(0).getSerializationFormatOption(),sameInstance(serializationFormatOption));
    }
}
