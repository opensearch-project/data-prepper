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
import org.opensearch.dataprepper.plugins.source.configuration.S3SelectCSVOption;
import org.opensearch.dataprepper.plugins.source.configuration.S3SelectOptions;
import org.opensearch.dataprepper.plugins.source.configuration.S3SelectSerializationFormatOption;
import org.opensearch.dataprepper.plugins.source.ownership.BucketOwnerProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompressionType;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
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

    @Mock
    private ResponseInputStream<GetObjectResponse> objectInputStream;

    @Mock
    private GetObjectResponse response;

    @Test
    public void testS3ScanServiceWithS3ObjectReferenceConfiguration() {
        String bucketName="my-bucket-1";
        String key = "file1.csv";
        String startDate = "2023-03-07T10:00:00";
        String range = "5d";
        final List<String> keyList = Arrays.asList(key);
        s3SourceConfig = mock(S3SourceConfig.class);
        s3Client = mock(S3Client.class);
        response = mock(GetObjectResponse.class);
        objectInputStream = mock(ResponseInputStream.class);
        s3ClientBuilderFactory = mock(S3ClientBuilderFactory.class);
        when(s3ClientBuilderFactory.getS3AsyncClient()).thenReturn(s3AsyncClient);
        when(s3ClientBuilderFactory.getS3Client()).thenReturn(s3Client);
        when(s3Client.getObject(any(GetObjectRequest.class))).thenReturn(objectInputStream);
        when(objectInputStream.response()).thenReturn(response);
        final Instant instantNow = Instant.now();
        when(response.lastModified()).thenReturn(instantNow);
        S3ScanScanOptions s3ScanScanOptions = mock(S3ScanScanOptions.class);
        when(s3ScanScanOptions.getStartTime()).thenReturn(startDate);
        when(s3ScanScanOptions.getRange()).thenReturn(range);
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
        pluginFactory = mock(PluginFactory.class);
        bucketOwnerProvider = mock(BucketOwnerProvider.class);
        when(bucketOwnerProvider.getBucketOwner(bucketName)).thenReturn(Optional.of(bucketName));
        S3ScanService service = new S3ScanService(s3SourceConfig,buffer,bucketOwnerProvider,eventConsumer,
                s3ObjectPluginMetrics,pluginFactory,s3ClientBuilderFactory);
        service.start();
        final List<ScanOptionsBuilder> scanOptionsBuilder = service.getScanOptions();
        assertThat(scanOptionsBuilder.get(0).getKeys(),sameInstance(keyList));
        assertThat(scanOptionsBuilder.get(0).getExpression(),nullValue());
        assertThat(scanOptionsBuilder.get(0).getBucket(),sameInstance(bucketName));
        assertThat(scanOptionsBuilder.get(0).getSerializationFormatOption(),nullValue());
        assertThat(scanOptionsBuilder.get(0).getCompressionOption(),sameInstance(CompressionOption.NONE));
        assertThat(scanOptionsBuilder.get(0).getRange(),sameInstance(range));
        assertThat(scanOptionsBuilder.get(0).getStartDate(),sameInstance(startDate));
        assertThat(scanOptionsBuilder.get(0).getExpressionType(),sameInstance("SQL"));
        assertThat(scanOptionsBuilder.get(0).getCompressionType(),sameInstance(CompressionType.NONE));
    }

    @Test
    public void testS3ScanServiceWithS3SelectConfiguration() {
        String bucketName="my-bucket-5";
        String key = "file1.csv";
        String startDate = "2023-03-07T10:00:00";
        String range = "2d";
        final List<String> keyList = Arrays.asList(key);
        String queryStatement = "select * from s3Object";
        S3SelectSerializationFormatOption serializationFormatOption = S3SelectSerializationFormatOption.CSV;
        s3SourceConfig = mock(S3SourceConfig.class);
        S3ScanScanOptions s3ScanScanOptions = mock(S3ScanScanOptions.class);
        when(s3ScanScanOptions.getStartTime()).thenReturn(startDate);
        when(s3ScanScanOptions.getRange()).thenReturn(range);
        S3ScanBucketOptions bucket = mock(S3ScanBucketOptions.class);
        final S3ScanBucketOption s3SelectOptions = mock(S3ScanBucketOption.class);
        final S3SelectOptions s3Select = mock(S3SelectOptions.class);
        final S3SelectCSVOption csvOption = mock(S3SelectCSVOption.class);
        when(s3Select.getExpressionType()).thenReturn("SQL");
        when(s3Select.getExpression()).thenReturn(queryStatement);
        when(s3Select.getS3SelectSerializationFormatOption()).thenReturn(serializationFormatOption);
        when(s3Select.getS3SelectCSVOption()).thenReturn(csvOption);
        when(s3Select.getCompressionType()).thenReturn("none");
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
        assertThat(scanOptionsBuilder.get(0).getKeys(),sameInstance(keyList));
        assertThat(scanOptionsBuilder.get(0).getExpression(),sameInstance(queryStatement));
        assertThat(scanOptionsBuilder.get(0).getBucket(),sameInstance(bucketName));
        assertThat(scanOptionsBuilder.get(0).getCompressionOption(),sameInstance(CompressionOption.NONE));
        assertThat(scanOptionsBuilder.get(0).getRange(),sameInstance(range));
        assertThat(scanOptionsBuilder.get(0).getStartDate(),sameInstance(startDate));
        assertThat(scanOptionsBuilder.get(0).getExpressionType(),sameInstance("SQL"));
        assertThat(scanOptionsBuilder.get(0).getCompressionType(),sameInstance(CompressionType.NONE));
    }
}
