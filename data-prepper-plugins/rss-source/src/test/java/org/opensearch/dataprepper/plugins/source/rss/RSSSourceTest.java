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
import org.mockserver.model.MediaType;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.document.Document;
import org.opensearch.dataprepper.model.record.Record;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

@ExtendWith(MockitoExtension.class)
@ExtendWith(MockServerExtension.class)
class RSSSourceTest {

    private final String PLUGIN_NAME = "rss";

    private final String PIPELINE_NAME = "test";

    @Mock
    private Buffer<Record<Document>> buffer;

    @Mock
    private RSSSourceConfig rssSourceConfig;

    private PluginMetrics pluginMetrics;

    private RSSSource rssSource;
    private Duration pollingFrequency;

    private final ClientAndServer client;
    private final String rssMockUrl;

    public RSSSourceTest(ClientAndServer client) {
        this.client = client;
        this.rssMockUrl= "http://localhost:"+client.getPort()+"/latest.rss";

    }


    @BeforeEach
    void setUp() throws IOException {
        pluginMetrics = PluginMetrics.fromNames(PLUGIN_NAME, PIPELINE_NAME);
        pollingFrequency = Duration.ofMillis(1800);
        lenient().when(rssSourceConfig.getUrl()).thenReturn(rssMockUrl);
        lenient().when(rssSourceConfig.getPollingFrequency()).thenReturn(pollingFrequency);
        rssSource = new RSSSource(pluginMetrics, rssSourceConfig);
        client.reset();
        var rssContent = IOUtils.resourceToByteArray("rss.xml",RSSSourceTest.class.getClassLoader());
        client
                .when(
                        request()
                                .withMethod("GET")
                                .withPath("/latest.rss")
                )
                .respond(
                        response()
                                .withContentType(new MediaType("application", "rss+xml", Map.of("charset","utf-8")))
                                .withBody(rssContent)

                );
    }

    @AfterEach
    public void tearDown() {
        rssSource.stop();
    }

    @Test
    void test_ExecutorService_keep_writing_Events_to_Buffer() throws Exception {
        rssSource.start(buffer);
        await().atMost(pollingFrequency.multipliedBy(2))
                        .untilAsserted(() -> {
                            verify(buffer, atLeastOnce()).writeAll(anyCollection(), anyInt());
                        });
        verify(buffer, atLeastOnce()).writeAll(anyCollection(), anyInt());
        await().atMost(pollingFrequency.multipliedBy(2))
                .untilAsserted(() -> {
                    verify(buffer, atLeast(2)).writeAll(anyCollection(), anyInt());
                });
        verify(buffer, atLeast(2)).writeAll(anyCollection(), anyInt());
    }
}
