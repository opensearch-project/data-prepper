/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rss;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.junit.jupiter.MockServerExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.rss.config.FeedConfig;
import org.opensearch.dataprepper.plugins.source.rss.config.FeedSourceConfig;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;

import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

@ExtendWith(MockitoExtension.class)
@ExtendWith(MockServerExtension.class)
class FeedSourceIT {

    @Mock
    private Buffer<Record<Event>> buffer;

    private final ClientAndServer client;
    private final String feedUrl;
    private FeedSource source;

    FeedSourceIT(final ClientAndServer client) {
        this.client = client;
        this.feedUrl = "http://localhost:" + client.getPort() + "/latest.rss";
    }

    @BeforeEach
    void setUp() throws IOException {
        client.reset();
        final byte[] rss = IOUtils.resourceToByteArray("rss.xml", FeedSourceIT.class.getClassLoader());
        client.when(request().withMethod("GET").withPath("/latest.rss"))
              .respond(response().withBody(rss));

        final FeedConfig feed = mock(FeedConfig.class);
        when(feed.getUrl()).thenReturn(feedUrl);

        final FeedSourceConfig config = mock(FeedSourceConfig.class);
        when(config.getFeeds()).thenReturn(Map.of("opensearch-forum", feed));
        when(config.getWorkers()).thenReturn(1);
        when(config.resolvePollingFrequency(feed)).thenReturn(Duration.ofMillis(500));

        source = new FeedSource(PluginMetrics.fromNames("rss", "test"), config);
    }

    @AfterEach
    void tearDown() {
        source.stop();
    }

    @Test
    void polls_feed_and_writes_events_to_buffer() throws Exception {
        source.start(buffer);
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                verify(buffer, atLeastOnce()).writeAll(anyCollection(), anyInt()));
    }
}
