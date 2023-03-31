/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source;

import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.codec.Codec;
import org.opensearch.dataprepper.plugins.source.compression.CompressionEngine;
import org.opensearch.dataprepper.plugins.source.configuration.S3SelectSerializationFormatOption;
import org.opensearch.dataprepper.plugins.source.ownership.BucketOwnerProvider;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompressionType;
import software.amazon.awssdk.services.s3.model.FileHeaderInfo;

import java.time.Duration;
import java.util.function.BiConsumer;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;

public class S3ObjectRequestTest {

    @Mock
    private Buffer<Record<Event>> buffer;
    private int numberOfRecordsToAccumulate;
    @Mock
    private Duration bufferTimeout;
    @Mock
    private S3ObjectPluginMetrics s3ObjectPluginMetrics;

    @Mock
    BiConsumer<Event, S3ObjectReference> eventConsumer;

    @Mock
    private Codec codec;

    @Mock
    private S3AsyncClient s3AsyncClient;

    @Mock
    private S3Client s3Client;
    @Mock
    private static final String queryStatement = "select _1 from s3Object";
    @Mock
    private S3SelectSerializationFormatOption serializationFormatOption;
    @Mock
    private CompressionEngine compressionEngine;
    @Mock
    private BucketOwnerProvider bucketOwnerProvider;
    @Mock
    private S3SelectResponseHandler s3SelectResponseHandler;
    @Test
    public void s3ScanObjectWorkerTest(){
        S3ObjectRequest request = new S3ObjectRequest.Builder(buffer,0,bufferTimeout,s3ObjectPluginMetrics)
                .codec(codec).eventConsumer(eventConsumer).
                s3AsyncClient(s3AsyncClient).
                s3Client(s3Client).
                serializationFormatOption(serializationFormatOption).
                compressionType(CompressionType.NONE).
                fileHeaderInfo(FileHeaderInfo.NONE).
                compressionEngine(compressionEngine).
                bucketOwnerProvider(bucketOwnerProvider).
                s3SelectResponseHandler(s3SelectResponseHandler).
                queryStatement(queryStatement).build();
        assertThat(request.getBuffer(),sameInstance(buffer));
        assertThat(request.getBufferTimeout(),sameInstance(bufferTimeout));
        assertThat(request.getEventConsumer(),sameInstance(eventConsumer));
        assertThat(request.getNumberOfRecordsToAccumulate(),equalTo(0));
        assertThat(request.getCodec(),sameInstance(codec));
        assertThat(request.getS3AsyncClient(),sameInstance(s3AsyncClient));
        assertThat(request.getS3Client(),sameInstance(s3Client));
        assertThat(request.getSerializationFormatOption(),sameInstance(serializationFormatOption));
        assertThat(request.getQueryStatement(),sameInstance(queryStatement));
        assertThat(request.getCompressionType(),sameInstance(CompressionType.NONE));
        assertThat(request.getFileHeaderInfo(),sameInstance(FileHeaderInfo.NONE));
        assertThat(request.getCompressionEngine(),sameInstance(compressionEngine));
        assertThat(request.getBucketOwnerProvider(),sameInstance(bucketOwnerProvider));
    }
}
