package org.opensearch.dataprepper.plugins.sink.s3.accumulator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.sink.s3.ownership.BucketOwnerProvider;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LocalFileBufferTest {

    public static final String BUCKET_NAME = UUID.randomUUID().toString();
    public static final String KEY = UUID.randomUUID().toString() + ".log";
    public static final String PREFIX = "local";
    public static final String SUFFIX = ".log";
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

    @Mock
    private BucketOwnerProvider bucketOwnerProvider;

    private LocalFileBuffer localFileBuffer;
    private File tempFile;

    private String defaultBucket;

    @BeforeEach
    void setUp() throws IOException {
        defaultBucket = UUID.randomUUID().toString();
        tempFile = File.createTempFile(PREFIX, SUFFIX);
        localFileBuffer = new LocalFileBuffer(tempFile, s3Client, bucketSupplier, keySupplier, defaultBucket, bucketOwnerProvider);
    }

    @Test
    void test_with_write_events_into_buffer() throws IOException {
        while (localFileBuffer.getEventCount() < 55) {
            OutputStream outputStream = localFileBuffer.getOutputStream();
            outputStream.write(generateByteArray());
            int eventCount = localFileBuffer.getEventCount() +1;
            localFileBuffer.setEventCount(eventCount);
        }
        assertThat(localFileBuffer.getSize(), greaterThan(1l));
        assertThat(localFileBuffer.getEventCount(), equalTo(55));
        assertThat(localFileBuffer.getDuration(), notNullValue());
        assertThat(localFileBuffer.getDuration(), greaterThanOrEqualTo(Duration.ZERO));
        localFileBuffer.flushAndCloseStream();
        localFileBuffer.removeTemporaryFile();
        assertFalse(tempFile.exists(), "The temp file has not been deleted.");
    }

    @Test
    void test_without_write_events_into_buffer() {
        assertThat(localFileBuffer.getSize(), equalTo(0L));
        assertThat(localFileBuffer.getEventCount(), equalTo(0));
        assertThat(localFileBuffer.getDuration(), notNullValue());
        assertThat(localFileBuffer.getDuration(), greaterThanOrEqualTo(Duration.ZERO));
        localFileBuffer.flushAndCloseStream();
        localFileBuffer.removeTemporaryFile();
        assertFalse(tempFile.exists(), "The temp file has not been deleted.");
    }

    @Test
    void test_with_write_events_into_buffer_and_flush_toS3() throws IOException {
        while (localFileBuffer.getEventCount() < 55) {
            OutputStream outputStream = localFileBuffer.getOutputStream();
            outputStream.write(generateByteArray());
            int eventCount = localFileBuffer.getEventCount() +1;
            localFileBuffer.setEventCount(eventCount);
        }
        assertThat(localFileBuffer.getSize(), greaterThan(1l));
        assertThat(localFileBuffer.getEventCount(), equalTo(55));
        assertThat(localFileBuffer.getDuration(), notNullValue());
        assertThat(localFileBuffer.getDuration(), greaterThanOrEqualTo(Duration.ZERO));

        when(keySupplier.get()).thenReturn(KEY);
        when(bucketSupplier.get()).thenReturn(BUCKET_NAME);

        final CompletableFuture<PutObjectResponse> expectedFuture = mock(CompletableFuture.class);
        when(expectedFuture.whenComplete(any(BiConsumer.class)))
                .thenReturn(expectedFuture);

        try (final MockedStatic<BufferUtilities> bufferUtilitiesMockedStatic = mockStatic(BufferUtilities.class)) {
            bufferUtilitiesMockedStatic.when(() ->
                            BufferUtilities.putObjectOrSendToDefaultBucket(eq(s3Client), any(AsyncRequestBody.class),
                                    eq(mockRunOnCompletion), eq(mockRunOnFailure), eq(KEY), eq(BUCKET_NAME), eq(defaultBucket), eq(bucketOwnerProvider)))
                    .thenReturn(expectedFuture);

            final Optional<CompletableFuture<?>> result = localFileBuffer.flushToS3(mockRunOnCompletion, mockRunOnFailure);
            assertThat(result.isPresent(), equalTo(true));
            assertThat(result.get(), equalTo(expectedFuture));

            final ArgumentCaptor<BiConsumer> biConsumer = ArgumentCaptor.forClass(BiConsumer.class);
            verify(expectedFuture).whenComplete(biConsumer.capture());

            final BiConsumer<PutObjectResponse, Throwable> actualBiConsumer = biConsumer.getValue();
            actualBiConsumer.accept(mock(PutObjectResponse.class), mock(RuntimeException.class));
        }

        assertFalse(tempFile.exists(), "The temp file has not been deleted.");
    }

    @AfterEach
    void cleanup() {
        tempFile.deleteOnExit();
    }

    private byte[] generateByteArray() {
        byte[] bytes = new byte[1000];
        for (int i = 0; i < 1000; i++) {
            bytes[i] = (byte) i;
        }
        return bytes;
    }
}