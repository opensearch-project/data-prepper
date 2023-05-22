package org.opensearch.dataprepper.plugins.fs;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.fs.LocalFilePositionOutputStream;

import java.io.IOException;
import java.io.RandomAccessFile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LocalFilePositionOutputStreamTest {

    private RandomAccessFile mockStream;
    private LocalFilePositionOutputStream outputStream;

    @BeforeEach
    public void setup() throws IOException {
        mockStream = mock(RandomAccessFile.class);
        outputStream = new LocalFilePositionOutputStream(mockStream);
    }

    @Test
    public void getPos_notClosed_returnsFilePointer() throws IOException {
        long mockPos = 100L;
        when(mockStream.getFilePointer()).thenReturn(mockPos);

        assertEquals(mockPos, outputStream.getPos());
        verify(mockStream, times(1)).getFilePointer();
    }

    @Test
    public void getPos_closed_returnsFileLength() throws IOException {
        long mockPos = 200L;
        when(mockStream.length()).thenReturn(mockPos);
        outputStream.close();

        assertEquals(mockPos, outputStream.getPos());
        verify(mockStream, times(1)).length();
        verify(mockStream, times(1)).close();
    }

    @Test
    public void write_bytes_passesThroughToStream() throws IOException {
        byte[] data = new byte[] { 0, 1, 2, 3 };
        outputStream.write(data);

        verify(mockStream, times(1)).write(data);
    }

    @Test
    public void write_bytesOffsetLength_passesThroughToStream() throws IOException {
        byte[] data = new byte[] { 0, 1, 2, 3 };
        int offset = 1;
        int length = 2;
        outputStream.write(data, offset, length);

        verify(mockStream, times(1)).write(data, offset, length);
    }

    @Test
    public void write_int_passesThroughToStream() throws IOException {
        int data = 123;
        outputStream.write(data);

        verify(mockStream, times(1)).write(data);
    }

    @Test
    public void close_closesStreamAndSetsClosed() throws IOException {
        outputStream.close();

        assertTrue(outputStream.isClosed());
        verify(mockStream, times(1)).close();
    }
}
