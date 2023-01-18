/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rss;

import com.apptasticsoftware.rssreader.Item;
import com.apptasticsoftware.rssreader.RssReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.document.Document;
import org.opensearch.dataprepper.model.record.Record;

import java.io.IOException;
import java.util.Collection;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RssReaderTaskTest {

    private RssReaderTask readerTask;

    private String url;

    private Item item1;

    private Item item2;

    @Mock
    private RssReader rssReader;

    @Mock
    private Buffer<Record<Document>> buffer;

    @Captor
    ArgumentCaptor<Collection<Record<Document>>> argumentCaptor;

    @BeforeEach
    void setUp() {
        url = UUID.randomUUID().toString();
        item1 = new Item();
        item2 = new Item();
        readerTask = new RssReaderTask(rssReader, url, buffer);
    }

    @Test
    void test_when_RssReader_throws_exception() throws IOException {
        when(rssReader.read(url)).thenThrow(IOException.class);
        assertThrows(RuntimeException.class, () -> readerTask.run());
        verifyNoInteractions(buffer);
    }

    @Test
    void test_when_itemStream_is_empty() throws IOException {
        when(rssReader.read(url)).thenReturn(Stream.empty());
        readerTask.run();
        verify(rssReader, times(1)).read(url);
        verifyNoInteractions(buffer);
    }

    @Test
    void test_when_itemStream_contains_one_item() throws Exception {
        when(rssReader.read(url)).thenReturn(Stream.of(item1));
        readerTask.run();
        verify(rssReader, times(1)).read(url);
        verify(buffer, times(1)).writeAll(argumentCaptor.capture(), anyInt());
        Collection<Record<Document>> collection = argumentCaptor.getValue();
        assertEquals(collection.size(), 1);
    }

    @Test
    void test_when_itemStream_contains_two_items() throws Exception {
        when(rssReader.read(url)).thenReturn(Stream.of(item1, item2));
        readerTask.run();
        verify(rssReader, times(1)).read(url);
        verify(buffer, times(1)).writeAll(argumentCaptor.capture(), anyInt());
        Collection<Record<Document>> collection = argumentCaptor.getValue();
        assertEquals(collection.size(), 2);
    }
}
