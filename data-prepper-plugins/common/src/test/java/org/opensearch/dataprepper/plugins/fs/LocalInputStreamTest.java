package org.opensearch.dataprepper.plugins.fs;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.EOFException;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class LocalInputStreamTest {

    private File testDataFile;
    private LocalInputStream localInputStream;

    @BeforeEach
    public void setup() throws Exception {
        testDataFile = File.createTempFile( "LocalFilePositionOutputStreamTest-", "txt");
        testDataFile.deleteOnExit();

        final String inputString = "a".repeat(100);
        final byte[] inputBytes = inputString.getBytes(StandardCharsets.UTF_8);
        Files.write(testDataFile.toPath(), inputBytes);

        localInputStream = LocalInputStream.create(testDataFile);
    }

    @Test
    public void read_byteArray_offset_length_callsInputRead() throws Exception {
        byte[] buffer = new byte[5];
        int offset = 0;
        int length = 5;
        localInputStream.read(buffer, offset, length);

        final String actualContent = new String(buffer, StandardCharsets.UTF_8);

        assertEquals("aaaaa", actualContent);
    }

    @Test
    public void skip_skipsTheGivenAmountAndReturnsTheDifferenceInPosition() throws Exception {
        assertEquals(5, localInputStream.skip(5));
    }

    @Test
    public void skip_skipsNegativeDoesNothing() throws Exception {
        assertEquals(0, localInputStream.skip(-5));
    }

    @Test
    public void mark_setsMarkPos() throws Exception {
        assertEquals(10, localInputStream.skip(10));
        localInputStream.mark(0);
        assertEquals(10L, localInputStream.getMarkedPos());
    }

    @Test
    public void reset_resetsToMarkPos() throws Exception {
        assertEquals(10, localInputStream.skip(10));
        localInputStream.mark(0);

        localInputStream.reset();
        assertEquals(10, localInputStream.getPos());
    }

    @Test
    public void getPos_returnsCurrentPos() throws Exception {
        assertEquals(10, localInputStream.skip(10));
        assertEquals(10, localInputStream.getPos());
    }

    @Test
    public void seek_changesPosition() throws Exception {
        localInputStream.seek(10L);
        assertEquals(10, localInputStream.getPos());
    }

    @Test
    public void readFully_bytes_callsInputReadFully() throws Exception {
        byte[] buffer = new byte[5];
        localInputStream.readFully(buffer);

        assertEquals("aaaaa", new String(buffer, StandardCharsets.UTF_8));
    }

    @Test
    public void read_singleByte_callsInputRead() throws Exception {
        int byteRead = localInputStream.read();
        byte b = (byte) (byteRead & 0xFF);

        assertEquals("a", new String(new byte[]{ b }, StandardCharsets.UTF_8));

    }

    @Test
    public void read_byteArray_callsInputRead() throws Exception {
        byte[] buffer = new byte[5];
        localInputStream.read(buffer);

        assertEquals("aaaaa", new String(buffer, StandardCharsets.UTF_8));
    }

    @Test
    public void readFully_bytes_offset_length_callsInputReadFully() throws Exception {
        byte[] buffer = new byte[5];
        localInputStream.readFully(buffer, 0, 5);

        assertEquals("aaaaa", new String(buffer, StandardCharsets.UTF_8));
    }

    @Test
    public void readDirectBuffer_readsIntoByteBuffer() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(100);
        localInputStream.read(buffer);
        buffer.flip();

        final String expectedContent = "a".repeat(100);
        assertEquals(expectedContent, new String(buffer.array(), StandardCharsets.UTF_8));
    }

    @Test
    public void readFullyDirectBuffer_readsFullyIntoByteBuffer() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(100);
        localInputStream.readFully(buffer);
        buffer.flip();

        final String expectedContent = "a".repeat(100);
        assertEquals(expectedContent, new String(buffer.array(), StandardCharsets.UTF_8));
    }

    @Test
    public void readFullyDirectBuffer_eofBeforeFullRead_throwsEOFException() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(8192);

        assertThrows(EOFException.class, () -> localInputStream.readFully(buffer));
    }

}