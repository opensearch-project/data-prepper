/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.s3;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.buffer.common.BufferAccumulator;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.s3.ownership.BucketOwnerProvider;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

@ExtendWith(MockitoExtension.class)
class S3ObjectMetadataWorkerTest {

    @Mock
    private S3Client s3Client;

    @Mock
    private Buffer<Record<Event>> buffer;

    @Mock
    private BucketOwnerProvider bucketOwnerProvider;

    @Mock
    private AcknowledgementSet acknowledgementSet;

    @Mock
    private Counter s3ObjectsSucceededCounter;

    @Mock
    private DistributionSummary s3ObjectEventsSummary;

    @Mock
    private DistributionSummary s3ObjectSizeSummary;
    @Mock
    private HeadObjectResponse headObjectResponse;
    @Mock
    private S3ObjectPluginMetrics s3ObjectPluginMetrics;
    @Mock
    private Timer s3ObjectReadTimer;

    private Duration bufferTimeout;
    private int recordsToAccumulate;
    private AtomicInteger recordsWritten;
    private int numEventsAdded;
    private String bucketName;
    private String key;
    private Exception exceptionThrownByCallable;

    private Record<Event> receivedRecord;
    @Mock
    private S3ObjectReference s3ObjectReference;
    @Mock
    private Counter s3ObjectsFailedCounter;

    private S3ObjectMetadataWorker createObjectUnderTest(final S3ObjectPluginMetrics s3ObjectPluginMetrics) {
        final S3ObjectRequest request = new S3ObjectRequest
                .Builder(buffer, recordsToAccumulate, bufferTimeout, s3ObjectPluginMetrics)
                .bucketOwnerProvider(bucketOwnerProvider)
                .s3Client(s3Client)
                .build();
        return new S3ObjectMetadataWorker(request);
    }

    @Test
    void S3ObjectMetadataWorkerTest_testEvents() throws Exception {
        Random random = new Random();
        Instant testTime = Instant.now();
        long objectSize = random.nextInt(100_000) + 10_000;
        bucketName = UUID.randomUUID().toString();
        key = UUID.randomUUID().toString();
        when(s3ObjectReference.getBucketName()).thenReturn(bucketName);
        when(s3ObjectReference.getKey()).thenReturn(key);
        when(headObjectResponse.lastModified()).thenReturn(testTime);
        when(headObjectResponse.contentLength()).thenReturn(objectSize);
        acknowledgementSet = mock(AcknowledgementSet.class);
        doAnswer(a -> {
            numEventsAdded++;
            return null;
        }).when(acknowledgementSet).add(any(Event.class));
        exceptionThrownByCallable = null;
        when(s3ObjectReadTimer.recordCallable(any(Callable.class)))
                .thenAnswer(a -> {
                    try {
                        a.getArgument(0, Callable.class).call();
                    } catch (final Exception ex) {
                        exceptionThrownByCallable = ex;
                        throw ex;
                    }
                    return null;
                });
        when(s3ObjectPluginMetrics.getS3ObjectReadTimer()).thenReturn(s3ObjectReadTimer);
        when(s3Client.headObject(any(HeadObjectRequest.class))).thenReturn(headObjectResponse);
        recordsWritten = new AtomicInteger(0);
        when(s3ObjectPluginMetrics.getS3ObjectEventsSummary()).thenReturn(s3ObjectEventsSummary);
        when(s3ObjectPluginMetrics.getS3ObjectsSucceededCounter()).thenReturn(s3ObjectsSucceededCounter);
        when(s3ObjectPluginMetrics.getS3ObjectSizeSummary()).thenReturn(s3ObjectSizeSummary);
        final BufferAccumulator bufferAccumulator = mock(BufferAccumulator.class);
        doAnswer(a -> {
            receivedRecord = a.getArgument(0);
            recordsWritten.incrementAndGet();
            return null;
        }).when(bufferAccumulator).add(any(Record.class));

        doAnswer(a -> {
            return recordsWritten.get();
        }).when(bufferAccumulator).getTotalWritten();

        try (final MockedStatic<BufferAccumulator> bufferAccumulatorMockedStatic = mockStatic(BufferAccumulator.class)) {
            bufferAccumulatorMockedStatic.when(() -> BufferAccumulator.create(buffer, recordsToAccumulate, bufferTimeout))
                    .thenReturn(bufferAccumulator);
            createObjectUnderTest(s3ObjectPluginMetrics).processS3Object(s3ObjectReference, acknowledgementSet, null, null);
            assertThat(recordsWritten.get(), equalTo(1));
            Event event = receivedRecord.getData();
            assertThat(event.get("bucket", String.class), equalTo(bucketName));
            assertThat(event.get("key", String.class), equalTo(key));
            assertThat(event.get("length", Long.class), equalTo(objectSize));
            assertThat(event.get("time", Instant.class), equalTo(testTime));
        }
    }
}
