/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.opensearch;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginConfigObservable;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.model.source.coordinator.SourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.UsesSourceCoordination;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.plugins.common.opensearch.ServerlessOptionsFactory;
import org.opensearch.dataprepper.plugins.common.opensearch.ServerlessNetworkPolicyUpdater;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.ServerlessOptions;
import org.opensearch.dataprepper.plugins.common.opensearch.ServerlessNetworkPolicyUpdaterFactory;
import org.opensearch.dataprepper.plugins.source.opensearch.metrics.OpenSearchSourcePluginMetrics;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.OpenSearchClientFactory;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.SearchAccessor;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.SearchAccessorStrategy;

import java.util.Objects;
import java.util.Optional;

@DataPrepperPlugin(name="opensearch", pluginType = Source.class, pluginConfigurationType = OpenSearchSourceConfiguration.class)
public class OpenSearchSource implements Source<Record<Event>>, UsesSourceCoordination {

    private final AwsCredentialsSupplier awsCredentialsSupplier;
    private final OpenSearchSourceConfiguration openSearchSourceConfiguration;
    private final AcknowledgementSetManager acknowledgementSetManager;
    private final PluginMetrics pluginMetrics;
    private final PluginConfigObservable pluginConfigObservable;

    private SourceCoordinator<OpenSearchIndexProgressState> sourceCoordinator;
    private OpenSearchService openSearchService;

    @DataPrepperPluginConstructor
    public OpenSearchSource(final OpenSearchSourceConfiguration openSearchSourceConfiguration,
                            final AwsCredentialsSupplier awsCredentialsSupplier,
                            final AcknowledgementSetManager acknowledgementSetManager,
                            final PluginMetrics pluginMetrics,
                            final PluginConfigObservable pluginConfigObservable) {
        this.openSearchSourceConfiguration = openSearchSourceConfiguration;
        this.awsCredentialsSupplier = awsCredentialsSupplier;
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.pluginMetrics = pluginMetrics;
        this.pluginConfigObservable = pluginConfigObservable;

        openSearchSourceConfiguration.validateAuthConfigConflictWithDeprecatedUsernameAndPassword();
        openSearchSourceConfiguration.validateAwsConfigWithUsernameAndPassword();
    }

    @Override
    public void start(final Buffer<Record<Event>> buffer) {
        if (buffer == null) {
            throw new IllegalStateException("Buffer provided is null");
        }
        maybeUpdateNetworkPolicy();
        startProcess(openSearchSourceConfiguration, buffer);
    }

    private void startProcess(final OpenSearchSourceConfiguration openSearchSourceConfiguration,
                              final Buffer<Record<Event>> buffer)  {
        final OpenSearchClientFactory openSearchClientFactory = OpenSearchClientFactory.create(awsCredentialsSupplier);
        final OpenSearchSourcePluginMetrics openSearchSourcePluginMetrics = OpenSearchSourcePluginMetrics.create(pluginMetrics);
        final SearchAccessorStrategy searchAccessorStrategy = SearchAccessorStrategy.create(
                openSearchSourcePluginMetrics, openSearchSourceConfiguration, openSearchClientFactory, pluginConfigObservable);

        final SearchAccessor searchAccessor = searchAccessorStrategy.getSearchAccessor();

        openSearchService = OpenSearchService.createOpenSearchService(searchAccessor, sourceCoordinator, openSearchSourceConfiguration, buffer, acknowledgementSetManager, openSearchSourcePluginMetrics);
        openSearchService.start();
    }

    @Override
    public boolean areAcknowledgementsEnabled() {
        return openSearchSourceConfiguration.isAcknowledgmentsEnabled();
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

    // VisibleForTesting
    void maybeUpdateNetworkPolicy() {
        final Optional<ServerlessOptions> maybeServerlessOptions = ServerlessOptionsFactory.create(
            openSearchSourceConfiguration.getAwsAuthenticationOptions());
        if (maybeServerlessOptions.isPresent()) {
            final ServerlessNetworkPolicyUpdater networkPolicyUpdater = ServerlessNetworkPolicyUpdaterFactory.create(
                awsCredentialsSupplier, openSearchSourceConfiguration.getAwsAuthenticationOptions()
            );
            networkPolicyUpdater.updateNetworkPolicy(
                maybeServerlessOptions.get().getNetworkPolicyName(),
                maybeServerlessOptions.get().getCollectionName(),
                maybeServerlessOptions.get().getVpceId()
            );
        }
    }
}
