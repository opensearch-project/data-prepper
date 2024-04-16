/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3.accumulator;

import org.apache.parquet.io.PositionOutputStream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class InMemoryBufferTest {

    public static final int MAX_EVENTS = 55;
    @Mock
    private S3AsyncClient s3Client;
    @Mock
    private Supplier<String> bucketSupplier;
    @Mock
    private Supplier<String> keySupplier;

    @Mock
    private Consumer<Boolean> mockRunOnCompletion;

    @Mock
    private Consumer<Throwable> mockRunOnFailure;

    private InMemoryBuffer inMemoryBuffer;

    @Test
    void test_with_write_event_into_buffer() throws IOException {
        inMemoryBuffer = new InMemoryBuffer(s3Client, bucketSupplier, keySupplier, null);

        while (inMemoryBuffer.getEventCount() < MAX_EVENTS) {
            OutputStream outputStream = inMemoryBuffer.getOutputStream();
            outputStream.write(generateByteArray());
            int eventCount = inMemoryBuffer.getEventCount() +1;
            inMemoryBuffer.setEventCount(eventCount);
        }
        assertThat(inMemoryBuffer.getSize(), greaterThanOrEqualTo(54110L));
        assertThat(inMemoryBuffer.getEventCount(), equalTo(MAX_EVENTS));
        assertThat(inMemoryBuffer.getDuration(), notNullValue());
        assertThat(inMemoryBuffer.getDuration(), greaterThanOrEqualTo(Duration.ZERO));

    }

    @Test
    @Disabled("unstable")
    /**
     * There are 5 checkpoints in the tests as below
     *     |-----------upperBoundDuration-------------|
     * startTime --- stopWatchStart --- endTime --- checkpoint --- stopwatchGetDuration
     *                                 |-lowerBoundDuration-|
     *                       |------------inMemoryBuffer.Duration-------------|
     *  This test assumes the startTime and stopWatchStart are same, checkpoint and stopwatchGetDuration are same.
     *  However, they are not true at some systems.
     */
    void getDuration_provides_duration_within_expected_range() throws IOException, InterruptedException {
        Instant startTime = Instant.now();
        inMemoryBuffer = new InMemoryBuffer(s3Client, bucketSupplier, keySupplier, null);
        Instant endTime = Instant.now();


        while (inMemoryBuffer.getEventCount() < MAX_EVENTS) {
            OutputStream outputStream = inMemoryBuffer.getOutputStream();
            outputStream.write(generateByteArray());
            int eventCount = inMemoryBuffer.getEventCount() +1;
            inMemoryBuffer.setEventCount(eventCount);
        }
        Thread.sleep(100);

        Instant durationCheckpointTime = Instant.now();
        Duration duration = inMemoryBuffer.getDuration();
        assertThat(duration, notNullValue());

        Duration upperBoundDuration = Duration.between(startTime, durationCheckpointTime).truncatedTo(ChronoUnit.MILLIS);
        Duration lowerBoundDuration = Duration.between(endTime, durationCheckpointTime).truncatedTo(ChronoUnit.MILLIS);
        assertThat(duration, greaterThanOrEqualTo(lowerBoundDuration));
        assertThat(duration, lessThanOrEqualTo(upperBoundDuration));
    }

    @Test
    void test_flush_to_s3_success() {
        final String key = UUID.randomUUID().toString();
        final String bucket = UUID.randomUUID().toString();
        when(keySupplier.get()).thenReturn(key);
        when(bucketSupplier.get()).thenReturn(bucket);

        inMemoryBuffer = new InMemoryBuffer(s3Client, bucketSupplier, keySupplier, null);
        Assertions.assertNotNull(inMemoryBuffer);

        final CompletableFuture<PutObjectResponse> expectedFuture = mock(CompletableFuture.class);

        try (final MockedStatic<BufferUtilities> bufferUtilitiesMockedStatic = mockStatic(BufferUtilities.class)) {
            bufferUtilitiesMockedStatic.when(() ->
                            BufferUtilities.putObjectOrSendToDefaultBucket(eq(s3Client), any(AsyncRequestBody.class),
                                    eq(mockRunOnCompletion), eq(mockRunOnFailure), eq(key), eq(bucket), eq(null)))
                            .thenReturn(expectedFuture);

            final Optional<CompletableFuture<?>> result = inMemoryBuffer.flushToS3(mockRunOnCompletion, mockRunOnFailure);
            assertThat(result.isPresent(), equalTo(true));
            assertThat(result.get(), equalTo(expectedFuture));

        }

    }

    @Test
    void getOutputStream_is_PositionOutputStream() {
        inMemoryBuffer = new InMemoryBuffer(s3Client, bucketSupplier, keySupplier, null);

        assertThat(inMemoryBuffer.getOutputStream(), instanceOf(PositionOutputStream.class));
    }

    @Test
    void getOutputStream_getPos_equals_written_size() throws IOException {
        inMemoryBuffer = new InMemoryBuffer(s3Client, bucketSupplier, keySupplier, null);

        while (inMemoryBuffer.getEventCount() < MAX_EVENTS) {
            OutputStream outputStream = inMemoryBuffer.getOutputStream();
            outputStream.write(generateByteArray());
            int eventCount = inMemoryBuffer.getEventCount() +1;
            inMemoryBuffer.setEventCount(eventCount);
        }

        PositionOutputStream outputStream = (PositionOutputStream) inMemoryBuffer.getOutputStream();
        assertThat(outputStream.getPos(), equalTo((long) MAX_EVENTS * 1000));

        assertThat(inMemoryBuffer.getOutputStream(), instanceOf(PositionOutputStream.class));
    }

    @Test
    void getSize_across_multiple_in_sequence() throws IOException {
        inMemoryBuffer = new InMemoryBuffer(s3Client, bucketSupplier, keySupplier, null);

        while (inMemoryBuffer.getEventCount() < MAX_EVENTS) {
            OutputStream outputStream = inMemoryBuffer.getOutputStream();
            outputStream.write(generateByteArray());
            int eventCount = inMemoryBuffer.getEventCount() +1;
            inMemoryBuffer.setEventCount(eventCount);
        }
        assertThat(inMemoryBuffer.getSize(), equalTo((long) MAX_EVENTS * 1000));

        inMemoryBuffer = new InMemoryBuffer(s3Client, bucketSupplier, keySupplier, null);
        assertThat(inMemoryBuffer.getSize(), equalTo(0L));

        while (inMemoryBuffer.getEventCount() < MAX_EVENTS) {
            OutputStream outputStream = inMemoryBuffer.getOutputStream();
            outputStream.write(generateByteArray());
            int eventCount = inMemoryBuffer.getEventCount() +1;
            inMemoryBuffer.setEventCount(eventCount);
        }
        assertThat(inMemoryBuffer.getSize(), equalTo((long) MAX_EVENTS * 1000));
    }


    private byte[] generateByteArray() {
        byte[] bytes = new byte[1000];
        for (int i = 0; i < 1000; i++) {
            bytes[i] = (byte) i;
        }
        return bytes;
    }
}