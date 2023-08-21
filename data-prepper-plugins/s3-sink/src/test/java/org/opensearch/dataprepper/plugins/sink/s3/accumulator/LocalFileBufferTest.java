package org.opensearch.dataprepper.plugins.sink.s3.accumulator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.UUID;
import java.util.function.Supplier;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LocalFileBufferTest {

    public static final String BUCKET_NAME = UUID.randomUUID().toString();
    public static final String KEY = UUID.randomUUID().toString() + ".log";
    public static final String PREFIX = "local";
    public static final String SUFFIX = ".log";
    @Mock
    private S3Client s3Client;
    @Mock
    private Supplier<String> bucketSupplier;
    @Mock
    private Supplier<String> keySupplier;
    private LocalFileBuffer localFileBuffer;
    private File tempFile;

    @BeforeEach
    void setUp() throws IOException {
        tempFile = File.createTempFile(PREFIX, SUFFIX);
        localFileBuffer = new LocalFileBuffer(tempFile, s3Client, bucketSupplier, keySupplier);
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
        assertThat(localFileBuffer.getDuration(), equalTo(0L));
        localFileBuffer.flushAndCloseStream();
        localFileBuffer.removeTemporaryFile();
        assertFalse(tempFile.exists(), "The temp file has not been deleted.");
    }

    @Test
    void test_without_write_events_into_buffer() {
        assertThat(localFileBuffer.getSize(), equalTo(0L));
        assertThat(localFileBuffer.getEventCount(), equalTo(0));
        assertThat(localFileBuffer.getDuration(), equalTo(0L));
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
        assertThat(localFileBuffer.getDuration(), greaterThanOrEqualTo(0L));

        when(keySupplier.get()).thenReturn(KEY);
        when(bucketSupplier.get()).thenReturn(BUCKET_NAME);

        assertDoesNotThrow(() -> {
            localFileBuffer.flushToS3();
        });

        ArgumentCaptor<PutObjectRequest> putObjectRequestArgumentCaptor = ArgumentCaptor.forClass(PutObjectRequest.class);
        verify(s3Client).putObject(putObjectRequestArgumentCaptor.capture(), any(RequestBody.class));
        PutObjectRequest actualRequest = putObjectRequestArgumentCaptor.getValue();

        assertThat(actualRequest, notNullValue());
        assertThat(actualRequest.bucket(), equalTo(BUCKET_NAME));
        assertThat(actualRequest.key(), equalTo(KEY));
        assertThat(actualRequest.expectedBucketOwner(), nullValue());

        assertFalse(tempFile.exists(), "The temp file has not been deleted.");
    }

    @Test
    void test_uploadedToS3_success() {
        when(keySupplier.get()).thenReturn(KEY);
        when(bucketSupplier.get()).thenReturn(BUCKET_NAME);

        Assertions.assertNotNull(localFileBuffer);
        assertDoesNotThrow(() -> {
            localFileBuffer.flushToS3();
        });

        ArgumentCaptor<PutObjectRequest> putObjectRequestArgumentCaptor = ArgumentCaptor.forClass(PutObjectRequest.class);
        verify(s3Client).putObject(putObjectRequestArgumentCaptor.capture(), any(RequestBody.class));
        PutObjectRequest actualRequest = putObjectRequestArgumentCaptor.getValue();

        assertThat(actualRequest, notNullValue());
        assertThat(actualRequest.bucket(), equalTo(BUCKET_NAME));
        assertThat(actualRequest.key(), equalTo(KEY));
        assertThat(actualRequest.expectedBucketOwner(), nullValue());

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