/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
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
import org.opensearch.dataprepper.plugins.source.rss.config.AuthenticationConfig;
import org.opensearch.dataprepper.plugins.source.rss.config.BasicAuthConfig;
import org.opensearch.dataprepper.plugins.source.rss.config.FeedConfig;
import org.opensearch.dataprepper.plugins.source.rss.config.FeedSourceConfig;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.Map;

import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockserver.model.Header.header;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;
import static org.mockserver.verify.VerificationTimes.atLeast;

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
    }

    @AfterEach
    void tearDown() {
        if (source != null) {
            source.stop();
        }
    }

    @Test
    void polls_feed_and_writes_events_to_buffer() {
        final FeedConfig feed = mock(FeedConfig.class);
        when(feed.getUrl()).thenReturn(feedUrl);

        final FeedSourceConfig config = mock(FeedSourceConfig.class);
        when(config.getFeeds()).thenReturn(Map.of("opensearch-forum", feed));
        when(config.getWorkers()).thenReturn(1);
        when(config.resolvePollingFrequency(feed)).thenReturn(Duration.ofMillis(500));

        source = new FeedSource(PluginMetrics.fromNames("rss", "test"), config);
        source.start(buffer);
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                verify(buffer, atLeastOnce()).writeAll(anyCollection(), anyInt()));
    }

    @Test
    void sends_basic_auth_header_when_feed_is_configured_with_credentials() {
        final BasicAuthConfig basic = mock(BasicAuthConfig.class);
        when(basic.getUsername()).thenReturn("user");
        when(basic.getPassword()).thenReturn("pass");
        final AuthenticationConfig authentication = mock(AuthenticationConfig.class);
        when(authentication.getBasic()).thenReturn(basic);
        final FeedConfig authenticatedFeed = mock(FeedConfig.class);
        when(authenticatedFeed.getUrl()).thenReturn(feedUrl);
        when(authenticatedFeed.getAuthentication()).thenReturn(authentication);

        final FeedSourceConfig authConfig = mock(FeedSourceConfig.class);
        when(authConfig.getFeeds()).thenReturn(Map.of("authenticated", authenticatedFeed));
        when(authConfig.getWorkers()).thenReturn(1);
        when(authConfig.resolvePollingFrequency(authenticatedFeed)).thenReturn(Duration.ofMillis(500));

        source = new FeedSource(PluginMetrics.fromNames("rss", "test"), authConfig);
        source.start(buffer);

        final String expectedToken = Base64.getEncoder().encodeToString(
                "user:pass".getBytes(StandardCharsets.UTF_8));
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                client.verify(request().withMethod("GET").withPath("/latest.rss")
                        .withHeader(header("Authorization", "Basic " + expectedToken)), atLeast(1)));
    }
}
