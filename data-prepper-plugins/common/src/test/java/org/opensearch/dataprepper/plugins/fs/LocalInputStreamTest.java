package org.opensearch.dataprepper.plugins.fs;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.fs.LocalInputStream;

import java.io.EOFException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LocalInputStreamTest {

    private RandomAccessFile mockInput;
    private LocalInputStream localInputStream;

    @BeforeEach
    public void setup() throws Exception {
        mockInput = mock(RandomAccessFile.class);
        localInputStream = new LocalInputStream(mockInput);
    }

    @Test
    public void read_byteArray_offset_length_callsInputRead() throws Exception {
        byte[] buffer = new byte[5];
        int offset = 0;
        int length = 5;
        localInputStream.read(buffer, offset, length);

        verify(mockInput, times(1)).read(buffer, offset, length);
    }

    @Test
    public void skip_skipsTheGivenAmountAndReturnsTheDifferenceInPosition() throws Exception {
        when(mockInput.getFilePointer()).thenReturn(5L).thenReturn(10L);
        when(mockInput.length()).thenReturn(20L);

        assertEquals(5, localInputStream.skip(5));

        verify(mockInput, times(2)).getFilePointer();
        verify(mockInput, times(1)).length();
        verify(mockInput, times(1)).seek(10L);
    }

    @Test
    public void mark_setsMarkPos() throws Exception {
        when(mockInput.getFilePointer()).thenReturn(10L);
        localInputStream.mark(0);
        assertEquals(10L, localInputStream.getMarkedPos());
    }

    @Test
    public void reset_resetsToMarkPos() throws Exception {
        when(mockInput.getFilePointer()).thenReturn(10L);
        localInputStream.mark(100);
        localInputStream.reset();
        verify(mockInput, times(1)).seek(10L);
    }

    @Test
    public void getPos_returnsCurrentPos() throws Exception {
        when(mockInput.getFilePointer()).thenReturn(10L);
        assertEquals(10L, localInputStream.getPos());
    }

    @Test
    public void seek_changesPosition() throws Exception {
        localInputStream.seek(10L);
        verify(mockInput, times(1)).seek(10L);
    }

    @Test
    public void readFully_bytes_callsInputReadFully() throws Exception {
        byte[] buffer = new byte[5];
        localInputStream.readFully(buffer);
        verify(mockInput, times(1)).readFully(buffer);
    }

    @Test
    public void read_singleByte_callsInputRead() throws Exception {
        localInputStream.read();
        verify(mockInput, times(1)).read();
    }

    @Test
    public void read_byteArray_callsInputRead() throws Exception {
        byte[] buffer = new byte[5];
        localInputStream.read(buffer);
        verify(mockInput, times(1)).read(buffer);
    }

    @Test
    public void readFully_bytes_offset_length_callsInputReadFully() throws Exception {
        byte[] buffer = new byte[5];
        localInputStream.readFully(buffer, 0, 5);
        verify(mockInput, times(1)).readFully(buffer, 0, 5);
    }

    @Test
    public void readDirectBuffer_readsIntoByteBuffer() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(8192);
        byte[] page = new byte[10];
        byte[] data = "Some Data".getBytes();
        System.arraycopy(data, 0, page, 0, data.length);

        when(mockInput.read(page, 0, Math.min(buffer.remaining(), page.length)))
                .thenAnswer(invocation -> {
                    byte[] output = invocation.getArgument(0);
                    int len = Math.min(output.length, data.length);
                    System.arraycopy(data, 0, output, 0, len);
                    return len;
                });

        localInputStream.read(buffer);

        buffer.flip();
        assertEquals("Some Data", new String(buffer.array(), 0, buffer.limit()));
    }

    @Test
    public void readFullyDirectBuffer_readsFullyIntoByteBuffer() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(8192);
        byte[] page = new byte[10];
        byte[] data = "Some Data".getBytes();
        System.arraycopy(data, 0, page, 0, data.length);

        when(mockInput.read(page, 0, Math.min(buffer.remaining(), page.length)))
                .thenAnswer(invocation -> {
                    byte[] output = invocation.getArgument(0);
                    int len = Math.min(output.length, data.length);
                    System.arraycopy(data, 0, output, 0, len);
                    return len;
                });

        localInputStream.readFully(buffer);

        buffer.flip();
        assertEquals("Some Data", new String(buffer.array(), 0, buffer.limit()));
    }

    @Test
    public void readFullyDirectBuffer_eofBeforeFullRead_throwsEOFException() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(8192);
        byte[] page = new byte[10];
        byte[] data = "Short".getBytes();
        System.arraycopy(data, 0, page, 0, data.length);

        when(mockInput.read(page, 0, Math.min(buffer.remaining(), page.length)))
                .thenAnswer(invocation -> {
                    byte[] output = invocation.getArgument(0);
                    int len = Math.min(output.length, data.length);
                    System.arraycopy(data, 0, output, 0, len);
                    return len;
                })
                .thenReturn(-1);

        assertThrows(EOFException.class, () -> localInputStream.readFully(buffer));
    }

}