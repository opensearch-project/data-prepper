/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.opensearch;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.plugins.source.opensearch.service.ElasticSearchService;
import org.opensearch.dataprepper.plugins.source.opensearch.service.HostsService;
import org.opensearch.dataprepper.plugins.source.opensearch.service.OpenSearchService;

import java.time.Duration;

@DataPrepperPlugin(name="opensearch", pluginType = Source.class , pluginConfigurationType =OpenSearchSourceConfiguration.class )
public class OpenSearchSource implements Source<Record<Event>> {

    static final Duration DEFAULT_BUFFER_TIMEOUT = Duration.ofSeconds(10);
    static final int DEFAULT_NUMBER_OF_RECORDS_TO_ACCUMULATE = 100;

    private final OpenSearchSourceConfiguration openSearchSourceConfiguration;

    private OpenSearchSourceService openSearchSourceService;

    @DataPrepperPluginConstructor
    public OpenSearchSource(final OpenSearchSourceConfiguration openSearchSourceConfiguration) {
        this.openSearchSourceConfiguration = openSearchSourceConfiguration;
    }

    @Override
    public void start(Buffer<Record<Event>> buffer) {
        if (buffer == null) {
            throw new IllegalStateException("Buffer provided is null");
        }
        HostsService hostsService = new HostsService();
        OpenSearchClientBuilder clientBuilder = new OpenSearchClientBuilder();
        OpenSearchService openSearchService = new OpenSearchService(clientBuilder);
        ElasticSearchService elasticSearchService = new ElasticSearchService(clientBuilder);
        BufferAccumulator bufferAccumulator = BufferAccumulator.create(buffer,DEFAULT_NUMBER_OF_RECORDS_TO_ACCUMULATE, DEFAULT_BUFFER_TIMEOUT);
        openSearchSourceService = new OpenSearchSourceService(
                openSearchSourceConfiguration,
                hostsService,
                openSearchService,
                elasticSearchService,
                bufferAccumulator);
        openSearchSourceService.processHosts();
    }

    @Override
    public void stop() {
        openSearchSourceService.stop();
    }
}
