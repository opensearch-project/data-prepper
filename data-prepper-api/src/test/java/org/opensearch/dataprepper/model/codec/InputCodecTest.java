package org.opensearch.dataprepper.model.codec;

import org.apache.parquet.io.SeekableInputStream;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.io.InputFile;

import java.util.function.Consumer;
import java.io.IOException;
import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

public class InputCodecTest {
    private SeekableInputStream inputStream;
    private InputFile inputFile;
    private DecompressionEngine decompressionEngine;
    private boolean closeCalled;

    @Test
    void testParse_success() throws Exception {
        InputCodec objectUnderTest = new InputCodec() {
            @Override
            public void parse(InputStream inputStream, Consumer<Record<Event>> eventConsumer) throws IOException {
                
            }
        };

        inputFile = mock(InputFile.class);
        inputStream = mock(SeekableInputStream.class);
        decompressionEngine = mock(DecompressionEngine.class);
        when(inputFile.newStream()).thenReturn(inputStream);
        closeCalled = false;
        doAnswer(a -> {
            closeCalled = true;
            return null;
        }).when(inputStream).close();
        when(decompressionEngine.createInputStream(any(InputStream.class))).thenReturn(inputStream);
        objectUnderTest.parse(inputFile, decompressionEngine, rec -> {});
        assertTrue(closeCalled);
    }

    @Test
    void testParse_exception() throws Exception {
        InputCodec objectUnderTest = new InputCodec() {
            @Override
            public void parse(InputStream inputStream, Consumer<Record<Event>> eventConsumer) throws IOException {
                throw new RuntimeException("error");
            }
        };

        inputFile = mock(InputFile.class);
        inputStream = mock(SeekableInputStream.class);
        decompressionEngine = mock(DecompressionEngine.class);
        when(inputFile.newStream()).thenReturn(inputStream);
        closeCalled = false;
        doAnswer(a -> {
            closeCalled = true;
            return null;
        }).when(inputStream).close();
        when(decompressionEngine.createInputStream(any(InputStream.class))).thenReturn(inputStream);
        assertThrows(RuntimeException.class, () -> objectUnderTest.parse(inputFile, decompressionEngine, rec -> {}));
        assertTrue(closeCalled);
    }
}
