/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rss;

import com.apptasticsoftware.rssreader.Item;
import com.apptasticsoftware.rssreader.RssReader;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.document.Document;
import org.opensearch.dataprepper.model.document.JacksonDocument;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class RssReaderTask implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(RssReaderTask.class);

    private final RssReader rssReader;

    private final String url;

    private final Buffer<Record<Document>> buffer;

    final Collection<Record<Document>> collection = new HashSet<>();


    public RssReaderTask(RssReader rssReader, final String url, final Buffer<Record<Document>> buffer) {
        this.rssReader = rssReader;
        this.url = url;
        this.buffer = buffer;

    }

    @Override
    public void run() {
        final Stream<Item> itemStream;
        try {
            LOG.debug("Reading RSS Feed URL");
            itemStream = rssReader.read(url);
        } catch (IOException e) {
            throw new RuntimeException("Unable to read the RSS Feed URL", e);
        }
        final List<Item> items = itemStream.collect(Collectors.toList());
        if (items.isEmpty()) {
            return;
        }
        for (final Item item: items) {
            LOG.debug("Converting Feed Item with ID:{} to an Event Document", item.getGuid());
            final Record<Document> document = buildEventDocument(item);
            collection.add(document);
        }
        try {
            buffer.writeAll(collection, 500);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Record<Document> buildEventDocument(final Item item) {
        final JacksonDocument document = JacksonDocument.builder()
                .withData(item)
                .getThis()
                .build();
        return new Record<>(document);
    }
}
