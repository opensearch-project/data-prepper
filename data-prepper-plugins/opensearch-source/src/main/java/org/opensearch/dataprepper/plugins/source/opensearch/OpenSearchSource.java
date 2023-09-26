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
import org.opensearch.dataprepper.model.source.coordinator.SourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.UsesSourceCoordination;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.OpenSearchClientFactory;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.SearchAccessor;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.SearchAccessorStrategy;

import java.util.Objects;

@DataPrepperPlugin(name="opensearch", pluginType = Source.class, pluginConfigurationType = OpenSearchSourceConfiguration.class)
public class OpenSearchSource implements Source<Record<Event>>, UsesSourceCoordination {

    private final AwsCredentialsSupplier awsCredentialsSupplier;
    private final OpenSearchSourceConfiguration openSearchSourceConfiguration;

    private SourceCoordinator<OpenSearchIndexProgressState> sourceCoordinator;
    private OpenSearchService openSearchService;

    @DataPrepperPluginConstructor
    public OpenSearchSource(final OpenSearchSourceConfiguration openSearchSourceConfiguration,
                            final AwsCredentialsSupplier awsCredentialsSupplier) {
        this.openSearchSourceConfiguration = openSearchSourceConfiguration;
        this.awsCredentialsSupplier = awsCredentialsSupplier;

        openSearchSourceConfiguration.validateAwsConfigWithUsernameAndPassword();
    }

    @Override
    public void start(final Buffer<Record<Event>> buffer) {
        if (buffer == null) {
            throw new IllegalStateException("Buffer provided is null");
        }
        startProcess(openSearchSourceConfiguration, buffer);
    }

    private void startProcess(final OpenSearchSourceConfiguration openSearchSourceConfiguration, final Buffer<Record<Event>> buffer)  {

        final OpenSearchClientFactory openSearchClientFactory = OpenSearchClientFactory.create(awsCredentialsSupplier);
        final SearchAccessorStrategy searchAccessorStrategy = SearchAccessorStrategy.create(openSearchSourceConfiguration, openSearchClientFactory);

        final SearchAccessor searchAccessor = searchAccessorStrategy.getSearchAccessor();

        openSearchService = OpenSearchService.createOpenSearchService(searchAccessor, sourceCoordinator, openSearchSourceConfiguration, buffer);
        openSearchService.start();
    }

    @Override
    public void stop() {
        if (Objects.nonNull(openSearchService)) {
            openSearchService.stop();
        }
    }

    @Override
    public <T> void setSourceCoordinator(final SourceCoordinator<T> sourceCoordinator) {
        this.sourceCoordinator = (SourceCoordinator<OpenSearchIndexProgressState>) sourceCoordinator;
    }

    @Override
    public Class<?> getPartitionProgressStateClass() {
        return OpenSearchIndexProgressState.class;
    }
}
