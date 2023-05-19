/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.opensearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@DataPrepperPlugin(name="opensearch", pluginType = Source.class , pluginConfigurationType =OpenSearchSourceConfiguration.class )
public class OpenSearchSource implements Source<Record<Event>> {

    private static final Logger LOG = LoggerFactory.getLogger(OpenSearchSource.class);

    private static final String OPEN_SEARCH = "opensearch";

    private OpenSearchClient openSearchClient;

    private ElasticsearchClient elasticsearchClient;

    private final OpenSearchSourceConfiguration openSearchSourceConfiguration;

    @DataPrepperPluginConstructor
    public OpenSearchSource(final OpenSearchSourceConfiguration openSearchSourceConfiguration) {
        this.openSearchSourceConfiguration = openSearchSourceConfiguration;
    }

    @Override
    public void start(final  Buffer<Record<Event>> buffer) {
        if (buffer == null) {
            throw new IllegalStateException("Buffer provided is null");
        }
        startProcess(openSearchSourceConfiguration,buffer);
    }

    private void startProcess(final OpenSearchSourceConfiguration openSearchSourceConfiguration,final Buffer<Record<Event>> buffer)  {
        PrepareConnection prepareConnection = new PrepareConnection();

        try {
            SourceInfo sourceInfo = new SourceInfo();
            SourceInfoProvider sourceInfoProvider = new SourceInfoProvider();
            String datasource = sourceInfoProvider.getSourceInfo(openSearchSourceConfiguration);
            sourceInfo.setDataSource(datasource);
            LOG.info("Datasource is : {} ", sourceInfo.getDataSource());
            sourceInfo = sourceInfoProvider.checkStatus(openSearchSourceConfiguration, sourceInfo);
            if (Boolean.TRUE.equals(sourceInfo.getHealthStatus())) {
                if (OPEN_SEARCH.equalsIgnoreCase(datasource)) {
                    openSearchClient = prepareConnection.prepareOpensearchConnection(openSearchSourceConfiguration);
                } else {
                    elasticsearchClient = prepareConnection.prepareElasticSearchConnection(openSearchSourceConfiguration);

                }

            } else {
                BackoffService backoff = new BackoffService(openSearchSourceConfiguration.getMaxRetries());
                backoff.waitUntilNextTry();
                while (backoff.shouldRetry()) {
                    sourceInfo = sourceInfoProvider.checkStatus(openSearchSourceConfiguration,sourceInfo);
                    if (Boolean.TRUE.equals(sourceInfo.getHealthStatus())) {
                        backoff.doNotRetry();
                        break;
                    } else {
                          backoff.errorOccured();
                    }
                }

            }
        } catch ( Exception e ){
              LOG.error("Error occured while starting the process",e);
        }


    }

    @Override
    public void stop() {
        LOG.info("Process stopped");
      }
}
