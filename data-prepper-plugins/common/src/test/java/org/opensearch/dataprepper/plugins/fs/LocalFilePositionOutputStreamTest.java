package org.opensearch.dataprepper.plugins.fs;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LocalFilePositionOutputStreamTest {

    private File testDataFile;
    private LocalFilePositionOutputStream outputStream;

    @BeforeEach
    public void setup() throws IOException {
        testDataFile = File.createTempFile( "LocalFilePositionOutputStreamTest-", "txt");
        testDataFile.deleteOnExit();
        outputStream = LocalFilePositionOutputStream.create(testDataFile);
    }

    @AfterEach
    public void tearDown() throws IOException {
        outputStream.close();
    }

    @Test
    public void getPos_notClosed_returnsFilePointer() throws IOException {
        final String inputString = "a".repeat(100);
        final byte[] inputBytes = inputString.getBytes(StandardCharsets.UTF_8);

        outputStream.write(inputBytes);

        assertEquals(100, outputStream.getPos());
    }

    @Test
    public void getPos_closed_returnsFileLength() throws IOException {
        final String inputString = "a".repeat(100);
        final byte[] inputBytes = inputString.getBytes(StandardCharsets.UTF_8);

        outputStream.write(inputBytes);
        outputStream.close();

        assertEquals(100, outputStream.getPos());
    }

    @Test
    public void write_bytes_passesThroughToStream() throws IOException {
        final String inputString = "a".repeat(100);
        final byte[] inputBytes = inputString.getBytes(StandardCharsets.UTF_8);

        outputStream.write(inputBytes);
        outputStream.close();

        String actualContent = Files.readString(testDataFile.toPath());

        assertEquals(inputString, actualContent);
    }

    @Test
    public void write_bytesOffsetLength_passesThroughToStream() throws IOException {
        final String inputString = "a".repeat(100);
        final byte[] inputBytes = inputString.getBytes(StandardCharsets.UTF_8);
        final int offset = 1;
        final int length = 2;

        outputStream.write(inputBytes, offset, length);

        outputStream.close();

        final String actualContent = Files.readString(testDataFile.toPath());

        assertEquals("aa", actualContent);
    }

    @Test
    public void write_int_passesThroughToStream() throws IOException {
        int data = 123;

        outputStream.write(data);

        outputStream.close();

        final String stringContent = Files.readString(testDataFile.toPath());
        final byte[] bytesContent = stringContent.getBytes(StandardCharsets.UTF_8);
        int actualContent = bytesContent[0] & 0xFF;


        assertEquals(data, actualContent);
    }

    @Test
    public void close_closesStreamAndSetsClosed() throws IOException {
        outputStream.close();

        assertTrue(outputStream.isClosed());
    }
}
