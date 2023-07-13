package org.opensearch.dataprepper.plugins.source;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.source.ownership.BucketOwnerProvider;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.http.HttpStatusCode;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class S3InputStreamTest {

    private static final Duration RETRY_DELAY = Duration.ofMillis(10);

    private static final int RETRIES = 3;

    @Mock(lenient = true)
    private S3Client s3Client;
    @Mock(lenient = true)
    private S3ObjectReference s3ObjectReference;
    @Mock
    private BucketOwnerProvider bucketOwnerProvider;
    @Mock(lenient = true)
    private HeadObjectResponse metadata;
    @Mock(lenient = true)
    private S3ObjectPluginMetrics s3ObjectPluginMetrics;
    private DistributionSummary s3ObjectSizeProcessedSummary;
    private Counter s3ObjectsFailedNotFoundCounter;
    private Counter s3ObjectsFailedAccessDeniedCounter;
    private String bucketName;
    private String key;

    @BeforeEach
    void setUp() {
        s3ObjectSizeProcessedSummary = mock(DistributionSummary.class);
        s3ObjectsFailedNotFoundCounter = mock(Counter.class);
        s3ObjectsFailedAccessDeniedCounter = mock(Counter.class);

        bucketName = UUID.randomUUID().toString();
        key = UUID.randomUUID().toString();
        when(s3ObjectReference.getBucketName()).thenReturn(bucketName);
        when(s3ObjectReference.getKey()).thenReturn(key);
        when(metadata.contentLength()).thenReturn(1000L);
        when(s3ObjectPluginMetrics.getS3ObjectSizeProcessedSummary()).thenReturn(s3ObjectSizeProcessedSummary);
        when(s3ObjectPluginMetrics.getS3ObjectsFailedNotFoundCounter()).thenReturn(s3ObjectsFailedNotFoundCounter);
        when(s3ObjectPluginMetrics.getS3ObjectsFailedAccessDeniedCounter()).thenReturn(s3ObjectsFailedAccessDeniedCounter);
    }

    private S3InputStream createObjectUnderTest() {
        return new S3InputStream(
                s3Client, s3ObjectReference, bucketOwnerProvider, metadata, s3ObjectPluginMetrics, RETRY_DELAY, RETRIES);
    }

    @Test
    void testAvailable() throws IOException {
        InputStream inputStream = new ByteArrayInputStream("Test data".getBytes());
        when(s3Client.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class))).thenReturn(inputStream);
        final S3InputStream s3InputStream = createObjectUnderTest();
        createObjectUnderTest().seek(0);

        int availableBytes = s3InputStream.available();
        assertEquals(9, availableBytes);
    }

    @Test
    void testClose() throws IOException {
        final S3InputStream s3InputStream = createObjectUnderTest();
        s3InputStream.close();

        assertThrows(IllegalStateException .class, () -> s3InputStream.read());
    }

    @Test
    void testMarkAndReset() throws IOException {
        InputStream inputStream = new ByteArrayInputStream("Test data".getBytes());
        when(s3Client.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class))).thenReturn(inputStream);
        final S3InputStream s3InputStream = createObjectUnderTest();
        s3InputStream.seek(5);

        s3InputStream.mark(5);
        s3InputStream.read();
        s3InputStream.reset();

        assertEquals(5, s3InputStream.getPos());
    }

    @Test
    void testRead() throws IOException {
        InputStream inputStream = new ByteArrayInputStream("Test data".getBytes());
        when(s3Client.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class))).thenReturn(inputStream);
        final S3InputStream s3InputStream = createObjectUnderTest();
        s3InputStream.seek(0); // Force opening the stream

        int firstByte = s3InputStream.read();
        assertEquals('T', firstByte);

        s3InputStream.close();

        verify(s3ObjectSizeProcessedSummary).record(1.0);
    }

    @Test
    void read_will_read_from_S3_with_bucket_and_key_when_no_owner() throws IOException {
        final InputStream inputStream = new ByteArrayInputStream("Test data".getBytes());
        when(s3Client.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class))).thenReturn(inputStream);
        final S3InputStream s3InputStream = createObjectUnderTest();
        s3InputStream.seek(0); // Force opening the stream

        s3InputStream.read();
        s3InputStream.close();

        final ArgumentCaptor<GetObjectRequest> getObjectRequestArgumentCaptor = ArgumentCaptor.forClass(GetObjectRequest.class);
        verify(s3Client).getObject(getObjectRequestArgumentCaptor.capture(), any(ResponseTransformer.class));

        final GetObjectRequest getObjectRequest = getObjectRequestArgumentCaptor.getValue();
        assertAll(
                () -> assertThat(getObjectRequest.bucket(), equalTo(bucketName)),
                () -> assertThat(getObjectRequest.key(), equalTo(key)),
                () -> assertThat(getObjectRequest.expectedBucketOwner(), nullValue())
        );
    }

    @Test
    void read_will_read_from_S3_with_bucket_key_and_expected_owner_when_owner_defined() throws IOException {
        final String owner = UUID.randomUUID().toString();
        when(bucketOwnerProvider.getBucketOwner(bucketName)).thenReturn(Optional.of(owner));
        final InputStream inputStream = new ByteArrayInputStream("Test data".getBytes());
        when(s3Client.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class))).thenReturn(inputStream);
        final S3InputStream s3InputStream = createObjectUnderTest();
        s3InputStream.seek(0); // Force opening the stream

        s3InputStream.read();
        s3InputStream.close();

        final ArgumentCaptor<GetObjectRequest> getObjectRequestArgumentCaptor = ArgumentCaptor.forClass(GetObjectRequest.class);
        verify(s3Client).getObject(getObjectRequestArgumentCaptor.capture(), any(ResponseTransformer.class));

        final GetObjectRequest getObjectRequest = getObjectRequestArgumentCaptor.getValue();
        assertAll(
                () -> assertThat(getObjectRequest.bucket(), equalTo(bucketName)),
                () -> assertThat(getObjectRequest.key(), equalTo(key)),
                () -> assertThat(getObjectRequest.expectedBucketOwner(), equalTo(owner))
        );
    }

    @Test
    void testReadEndOfFile() throws IOException {
        InputStream inputStream = new ByteArrayInputStream("".getBytes());
        when(s3Client.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class))).thenReturn(inputStream);

        final S3InputStream s3InputStream = createObjectUnderTest();
        int firstByte = s3InputStream.read();
        assertEquals(-1, firstByte);

        s3InputStream.close();

        verify(s3ObjectSizeProcessedSummary).record(0.0);
    }

    @ParameterizedTest
    @MethodSource("retryableExceptions")
    void testReadSucceedsAfterRetries(final Class<? extends Exception> retryableExceptionClass) throws IOException {
        InputStream mockInputStream = mock(InputStream.class);
        when(s3Client.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class)))
            .thenReturn(mockInputStream);

        when(mockInputStream.read())
            .thenThrow(retryableExceptionClass)
            .thenReturn(1);

        final S3InputStream s3InputStream = createObjectUnderTest();
        int firstByte = s3InputStream.read();
        assertEquals(1, firstByte);

        s3InputStream.close();

        verify(s3ObjectSizeProcessedSummary).record(1.0);

        verify(mockInputStream, times(2)).read();
        verify(mockInputStream, times(2)).close();
        verify(s3Client, times(2))
            .getObject(any(GetObjectRequest.class), any(ResponseTransformer.class));
    }

    @ParameterizedTest
    @MethodSource("retryableExceptions")
    void testReadFailsAfterRetries(final Class<? extends Exception> retryableExceptionClass) throws IOException {
        InputStream mockInputStream = mock(InputStream.class);
        when(s3Client.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class)))
            .thenReturn(mockInputStream);

        when(mockInputStream.read()).thenThrow(retryableExceptionClass);

        final S3InputStream s3InputStream = createObjectUnderTest();
        assertThrows(IOException.class, () -> s3InputStream.read());

        s3InputStream.close();

        verify(mockInputStream, times(RETRIES + 1)).read();
        verify(mockInputStream, times(RETRIES + 2)).close();
        verify(s3Client, times(RETRIES + 2))
            .getObject(any(GetObjectRequest.class), any(ResponseTransformer.class));
    }

    @Test
    void testReadByteArray() throws IOException {
        InputStream inputStream = new ByteArrayInputStream("Test data".getBytes());
        when(s3Client.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class))).thenReturn(inputStream);
        final S3InputStream s3InputStream = createObjectUnderTest();
        s3InputStream.seek(0); // Force opening the stream

        byte[] buffer = new byte[4];
        int bytesRead = s3InputStream.read(buffer);

        assertEquals(4, bytesRead);
        assertArrayEquals("Test".getBytes(), buffer);

        s3InputStream.close();

        verify(s3ObjectSizeProcessedSummary).record(4.0);
    }

    @Test
    void testReadAllBytes() throws IOException {
        InputStream inputStream = new ByteArrayInputStream("Test data".getBytes());
        when(s3Client.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class))).thenReturn(inputStream);
        final S3InputStream s3InputStream = createObjectUnderTest();
        s3InputStream.seek(0); // Force opening the stream

        final byte[] buffer = s3InputStream.readAllBytes();

        assertArrayEquals("Test data".getBytes(), buffer);

        s3InputStream.close();
        verify(s3ObjectSizeProcessedSummary).record(9.0);
    }

    @ParameterizedTest
    @MethodSource("retryableExceptions")
    void testReadAllBytesSucceedsAfterRetries(final Class<? extends Exception> retryableExceptionClass)
        throws IOException
    {
        InputStream mockInputStream = mock(InputStream.class);
        when(s3Client.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class)))
            .thenReturn(mockInputStream);

        when(mockInputStream.readAllBytes())
            .thenThrow(retryableExceptionClass)
            .thenReturn("Test data".getBytes());

        final S3InputStream s3InputStream = createObjectUnderTest();
        final byte[] buffer = s3InputStream.readAllBytes();

        assertArrayEquals("Test data".getBytes(), buffer);

        s3InputStream.close();

        verify(s3ObjectSizeProcessedSummary).record(9.0);
        verify(mockInputStream, times(2)).readAllBytes();
        verify(mockInputStream, times(2)).close();
        verify(s3Client, times(2))
            .getObject(any(GetObjectRequest.class), any(ResponseTransformer.class));
    }

    @ParameterizedTest
    @MethodSource("retryableExceptions")
    void testReadAllBytesFailsAfterRetries(final Class<? extends Exception> retryableExceptionClass)
        throws IOException
    {
        InputStream mockInputStream = mock(InputStream.class);
        when(s3Client.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class)))
            .thenReturn(mockInputStream);

        when(mockInputStream.readAllBytes()).thenThrow(retryableExceptionClass);

        final S3InputStream s3InputStream = createObjectUnderTest();
        assertThrows(IOException.class, () -> s3InputStream.readAllBytes());

        s3InputStream.close();

        verify(mockInputStream, times(RETRIES + 1)).readAllBytes();
        verify(mockInputStream, times(RETRIES + 2)).close();
        verify(s3Client, times(RETRIES + 2))
            .getObject(any(GetObjectRequest.class), any(ResponseTransformer.class));
    }

    @Test
    void testReadNBytes_intoArray() throws Exception {
        InputStream inputStream = new ByteArrayInputStream("Test data".getBytes());
        when(s3Client.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class))).thenReturn(inputStream);
        final S3InputStream s3InputStream = createObjectUnderTest();
        s3InputStream.seek(0); // Force opening the stream

        byte[] buffer = new byte[9];
        int bytesRead = s3InputStream.readNBytes(buffer, 0, 4);

        assertEquals(4, bytesRead);
        assertArrayEquals("Test\u0000\u0000\u0000\u0000\u0000".getBytes(), buffer);

        s3InputStream.close();
        verify(s3ObjectSizeProcessedSummary).record(4.0);
    }

    @Test
    void testReadNBytes_endOfFile() throws Exception {
        InputStream inputStream = new ByteArrayInputStream("".getBytes());
        when(s3Client.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class))).thenReturn(inputStream);

        byte[] buffer = new byte[9];
        final S3InputStream s3InputStream = createObjectUnderTest();
        int bytesRead = s3InputStream.readNBytes(buffer, 0, 4);

        assertEquals(0, bytesRead);

        s3InputStream.close();
        verify(s3ObjectSizeProcessedSummary).record(0.0);
    }

    @Test
    void testReadNBytes_getArray() throws Exception {
        InputStream inputStream = new ByteArrayInputStream("Test data".getBytes());
        when(s3Client.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class))).thenReturn(inputStream);
        final S3InputStream s3InputStream = createObjectUnderTest();
        s3InputStream.seek(0); // Force opening the stream

        final byte[] buffer = s3InputStream.readNBytes(4);

        assertArrayEquals("Test".getBytes(), buffer);

        s3InputStream.close();
        verify(s3ObjectSizeProcessedSummary).record(4.0);
    }

    @Test
    void testSkip() throws IOException {
        InputStream inputStream = new ByteArrayInputStream("Test data".getBytes());
        when(s3Client.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class))).thenReturn(inputStream);
        final S3InputStream s3InputStream = createObjectUnderTest();
        s3InputStream.seek(0); // Force opening the stream

        long skippedBytes = s3InputStream.skip(5);

        assertEquals(5, skippedBytes);
        assertEquals(5, s3InputStream.getPos());

        s3InputStream.close();
        verify(s3ObjectSizeProcessedSummary).record(0.0);
    }

    @Test
    void testSeek() throws IOException {
        InputStream inputStream = new ByteArrayInputStream("Test data".getBytes());
        when(s3Client.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class))).thenReturn(inputStream);
        final S3InputStream s3InputStream = createObjectUnderTest();
        s3InputStream.read();

        s3InputStream.seek(5);
        assertEquals(5L, s3InputStream.getPos());
        char firstByte = (char) (s3InputStream.read() & 0xff);

        assertEquals('d', firstByte);
        assertEquals(6L, s3InputStream.getPos());

        s3InputStream.close();
        verify(s3ObjectSizeProcessedSummary).record(2.0);
    }

    @Test
    void testReadFullyByteArray() throws IOException {
        InputStream inputStream = new ByteArrayInputStream("Test data".getBytes());
        when(s3Client.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class))).thenReturn(inputStream);
        final S3InputStream s3InputStream = createObjectUnderTest();
        s3InputStream.seek(0); // Force opening the stream

        byte[] buffer = new byte[4];
        s3InputStream.readFully(buffer);

        assertArrayEquals("Test".getBytes(), buffer);

        s3InputStream.close();
        verify(s3ObjectSizeProcessedSummary).record(4.0);
    }

    @Test
    void testReadFullyByteArrayWithStartAndLength() throws IOException {
        InputStream inputStream = new ByteArrayInputStream("Test data".getBytes());
        when(s3Client.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class))).thenReturn(inputStream);
        final S3InputStream s3InputStream = createObjectUnderTest();
        s3InputStream.seek(0); // Force opening the stream

        byte[] buffer = new byte[6];
        s3InputStream.readFully(buffer, 1, 4);

        assertArrayEquals(new byte[]{0, 'T', 'e', 's', 't', 0}, buffer);

        s3InputStream.close();
        verify(s3ObjectSizeProcessedSummary).record(4.0);
    }

    @Test
    void testReadFullyByteBuffer() throws IOException {
        InputStream inputStream = new ByteArrayInputStream("Test data".getBytes());
        when(s3Client.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class))).thenReturn(inputStream);
        final S3InputStream s3InputStream = createObjectUnderTest();
        s3InputStream.seek(0); // Force opening the stream

        ByteBuffer buffer = ByteBuffer.allocate(4);
        s3InputStream.readFully(buffer);

        buffer.flip();
        assertEquals("Test", new String(buffer.array(), buffer.position(), buffer.remaining()));

        s3InputStream.close();
        verify(s3ObjectSizeProcessedSummary).record(4.0);
    }

    @Test
    void testReadFullyByteBuffer_endOfFile() throws IOException {
        InputStream inputStream = new ByteArrayInputStream("".getBytes());
        when(s3Client.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class))).thenReturn(inputStream);
        final S3InputStream s3InputStream = createObjectUnderTest();
        s3InputStream.seek(0); // Force opening the stream

        ByteBuffer buffer = ByteBuffer.allocate(4);
        assertThrows(IOException.class, () -> s3InputStream.readFully(buffer));

        s3InputStream.close();
        verify(s3ObjectSizeProcessedSummary).record(0.0);
    }

    @Test
    void testReadFullyHeapBuffer() throws IOException {
        InputStream inputStream = new ByteArrayInputStream("Test data".getBytes());
        when(s3Client.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class))).thenReturn(inputStream);
        final S3InputStream s3InputStream = createObjectUnderTest();
        s3InputStream.seek(0); // Force opening the stream

        ByteBuffer buffer = ByteBuffer.allocate(4);
        s3InputStream.readFully(buffer);

        buffer.flip();
        assertEquals("Test", new String(buffer.array(), buffer.position(), buffer.remaining()));

        s3InputStream.close();
        verify(s3ObjectSizeProcessedSummary).record(4.0);
    }

    @Test
    void testReadFullyDirectBuffer() throws IOException {
        InputStream inputStream = new ByteArrayInputStream("Test data".getBytes());
        when(s3Client.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class))).thenReturn(inputStream);
        final S3InputStream s3InputStream = createObjectUnderTest();
        s3InputStream.seek(0); // Force opening the stream

        ByteBuffer buffer = ByteBuffer.allocateDirect(4);
        s3InputStream.readFully(buffer);

        buffer.flip();
        byte[] byteArray = new byte[buffer.remaining()];
        buffer.get(byteArray);
        assertEquals("Test", new String(byteArray));

        s3InputStream.close();
        verify(s3ObjectSizeProcessedSummary).record(4.0);
    }

    @Test
    void testReadFromClosedFails() throws IOException {
        InputStream inputStream = new ByteArrayInputStream("Test data".getBytes());
        when(s3Client.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class))).thenReturn(inputStream);
        final S3InputStream s3InputStream = createObjectUnderTest();
        s3InputStream.seek(0); // Force opening the stream

        byte[] buffer = new byte[4];
        s3InputStream.readFully(buffer);

        s3InputStream.close();
        verify(s3ObjectSizeProcessedSummary).record(4.0);

        assertThrows(IllegalStateException.class, () -> s3InputStream.readAllBytes());
    }

    @Test
    void testReadAfterSeekBackwardsWorks() throws IOException {
        InputStream inputStream = new ByteArrayInputStream("Test data".getBytes());
        when(s3Client.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class))).thenReturn(inputStream);
        final S3InputStream s3InputStream = createObjectUnderTest();
        s3InputStream.seek(5); // Force opening the stream

        byte[] buffer = new byte[4];
        s3InputStream.readFully(buffer);

        assertEquals("Test", new String(buffer));

        s3InputStream.seek(0);
        buffer = new byte[4];
        s3InputStream.readFully(buffer);

        assertEquals(" dat", new String(buffer));

        s3InputStream.close();
        verify(s3ObjectSizeProcessedSummary).record(8.0);
    }

    @Test
    void testS3ObjectsFailedNotFoundCounter() {
        when(s3Client.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class))).thenThrow(
            S3Exception.builder()
                .statusCode(HttpStatusCode.NOT_FOUND)
                .build());

        final S3InputStream s3InputStream = createObjectUnderTest();
        assertThrows(IOException.class, () -> s3InputStream.read()); // Force opening the stream

        verify(s3ObjectsFailedNotFoundCounter).increment();
    }

    @Test
    void testS3ObjectsFailedAccessDeniedCounter() {
        when(s3Client.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class))).thenThrow(
            S3Exception.builder()
                .statusCode(HttpStatusCode.FORBIDDEN)
                .build());

        final S3InputStream s3InputStream = createObjectUnderTest();
        assertThrows(IOException.class, () -> s3InputStream.read()); // Force opening the stream

        verify(s3ObjectsFailedAccessDeniedCounter).increment();
    }

    private static Stream<Class<? extends Throwable>> retryableExceptions() {
        return S3InputStream.RETRYABLE_EXCEPTIONS.stream();
    }
}
