package org.opensearch.dataprepper.plugins.source;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.LongAdder;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class S3InputStreamTest {
    @Mock(lenient = true)
    private S3Client s3Client;
    @Mock(lenient = true)
    private S3ObjectReference s3ObjectReference;
    @Mock(lenient = true)
    private HeadObjectResponse metadata;

    private LongAdder bytesCounter;
    private S3InputStream s3InputStream;

    @BeforeEach
    void setUp() {
        when(s3ObjectReference.getBucketName()).thenReturn("test-bucket");
        when(s3ObjectReference.getKey()).thenReturn("test-key");

        when(metadata.contentLength()).thenReturn(1000L);

        bytesCounter = new LongAdder();
        s3InputStream = new S3InputStream(s3Client, s3ObjectReference, metadata, bytesCounter);
    }

    @Test
    void testAvailable() throws IOException {
        InputStream inputStream = new ByteArrayInputStream("Test data".getBytes());
        when(s3Client.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class))).thenReturn(inputStream);
        s3InputStream.seek(0);

        int availableBytes = s3InputStream.available();
        assertEquals(9, availableBytes);
    }

    @Test
    void testClose() throws IOException {
        s3InputStream.close();

        assertThrows(IllegalStateException .class, () -> s3InputStream.read());
    }

    @Test
    void testMarkAndReset() throws IOException {
        InputStream inputStream = new ByteArrayInputStream("Test data".getBytes());
        when(s3Client.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class))).thenReturn(inputStream);
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
        s3InputStream.seek(0); // Force opening the stream

        int firstByte = s3InputStream.read();
        assertEquals('T', firstByte);
        assertEquals(1, bytesCounter.intValue());
    }

    @Test
    void testReadByteArray() throws IOException {
        InputStream inputStream = new ByteArrayInputStream("Test data".getBytes());
        when(s3Client.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class))).thenReturn(inputStream);
        s3InputStream.seek(0); // Force opening the stream

        byte[] buffer = new byte[4];
        int bytesRead = s3InputStream.read(buffer);

        assertEquals(4, bytesRead);
        assertArrayEquals("Test".getBytes(), buffer);
    }

    @Test
    void testReadAllBytes() throws IOException {
        InputStream inputStream = new ByteArrayInputStream("Test data".getBytes());
        when(s3Client.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class))).thenReturn(inputStream);
        s3InputStream.seek(0); // Force opening the stream

        final byte[] buffer = s3InputStream.readAllBytes();

        assertArrayEquals("Test data".getBytes(), buffer);
    }

    @Test
    void testReadNBytes_intoArray() throws Exception {
        InputStream inputStream = new ByteArrayInputStream("Test data".getBytes());
        when(s3Client.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class))).thenReturn(inputStream);
        s3InputStream.seek(0); // Force opening the stream

        byte[] buffer = new byte[9];
        int bytesRead = s3InputStream.readNBytes(buffer, 0, 4);

        assertEquals(4, bytesRead);
        assertArrayEquals("Test\u0000\u0000\u0000\u0000\u0000".getBytes(), buffer);
    }

    @Test
    void testReadNBytes_getArray() throws Exception {
        InputStream inputStream = new ByteArrayInputStream("Test data".getBytes());
        when(s3Client.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class))).thenReturn(inputStream);
        s3InputStream.seek(0); // Force opening the stream

        final byte[] buffer = s3InputStream.readNBytes(4);

        assertArrayEquals("Test".getBytes(), buffer);
    }

    @Test
    void testSkip() throws IOException {
        InputStream inputStream = new ByteArrayInputStream("Test data".getBytes());
        when(s3Client.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class))).thenReturn(inputStream);
        s3InputStream.seek(0); // Force opening the stream

        long skippedBytes = s3InputStream.skip(5);

        assertEquals(5, skippedBytes);
        assertEquals(5, s3InputStream.getPos());
    }

    @Test
    void testSeek() throws IOException {
        InputStream inputStream = new ByteArrayInputStream("Test data".getBytes());
        when(s3Client.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class))).thenReturn(inputStream);
        s3InputStream.read();

        s3InputStream.seek(5);
        assertEquals(5L, s3InputStream.getPos());
        char firstByte = (char) (s3InputStream.read() & 0xff);

        assertEquals('d', firstByte);
        assertEquals(6L, s3InputStream.getPos());
    }

    @Test
    void testReadFullyByteArray() throws IOException {
        InputStream inputStream = new ByteArrayInputStream("Test data".getBytes());
        when(s3Client.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class))).thenReturn(inputStream);
        s3InputStream.seek(0); // Force opening the stream

        byte[] buffer = new byte[4];
        s3InputStream.readFully(buffer);

        assertArrayEquals("Test".getBytes(), buffer);
    }

    @Test
    void testReadFullyByteArrayWithStartAndLength() throws IOException {
        InputStream inputStream = new ByteArrayInputStream("Test data".getBytes());
        when(s3Client.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class))).thenReturn(inputStream);
        s3InputStream.seek(0); // Force opening the stream

        byte[] buffer = new byte[6];
        s3InputStream.readFully(buffer, 1, 4);

        assertArrayEquals(new byte[]{0, 'T', 'e', 's', 't', 0}, buffer);
    }

    @Test
    void testReadFullyByteBuffer() throws IOException {
        InputStream inputStream = new ByteArrayInputStream("Test data".getBytes());
        when(s3Client.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class))).thenReturn(inputStream);
        s3InputStream.seek(0); // Force opening the stream

        ByteBuffer buffer = ByteBuffer.allocate(4);
        s3InputStream.readFully(buffer);

        buffer.flip();
        assertEquals("Test", new String(buffer.array(), buffer.position(), buffer.remaining()));
    }

    @Test
    void testReadFullyHeapBuffer() throws IOException {
        InputStream inputStream = new ByteArrayInputStream("Test data".getBytes());
        when(s3Client.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class))).thenReturn(inputStream);
        s3InputStream.seek(0); // Force opening the stream

        ByteBuffer buffer = ByteBuffer.allocate(4);
        S3InputStream.readFullyHeapBuffer(inputStream, buffer);

        buffer.flip();
        assertEquals("Test", new String(buffer.array(), buffer.position(), buffer.remaining()));
    }

    @Test
    void testReadFullyDirectBuffer() throws IOException {
        InputStream inputStream = new ByteArrayInputStream("Test data".getBytes());
        when(s3Client.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class))).thenReturn(inputStream);
        s3InputStream.seek(0); // Force opening the stream

        ByteBuffer buffer = ByteBuffer.allocateDirect(4);
        S3InputStream.readFullyDirectBuffer(inputStream, buffer, new byte[4]);

        buffer.flip();
        byte[] byteArray = new byte[buffer.remaining()];
        buffer.get(byteArray);
        assertEquals("Test", new String(byteArray));
    }

    @Test
    void testReadFromClosedFails() throws IOException {
        InputStream inputStream = new ByteArrayInputStream("Test data".getBytes());
        when(s3Client.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class))).thenReturn(inputStream);
        s3InputStream.seek(0); // Force opening the stream

        byte[] buffer = new byte[4];
        s3InputStream.readFully(buffer);

        s3InputStream.close();

        assertThrows(IllegalStateException.class, () -> s3InputStream.readAllBytes());
    }

    @Test
    void testReadAfterSeekBackwardsWorks() throws IOException {
        InputStream inputStream = new ByteArrayInputStream("Test data".getBytes());
        when(s3Client.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class))).thenReturn(inputStream);
        s3InputStream.seek(5); // Force opening the stream

        byte[] buffer = new byte[4];
        s3InputStream.readFully(buffer);

        assertEquals("Test", new String(buffer));

        s3InputStream.seek(0);
        buffer = new byte[4];
        s3InputStream.readFully(buffer);

        assertEquals(" dat", new String(buffer));
    }
}
