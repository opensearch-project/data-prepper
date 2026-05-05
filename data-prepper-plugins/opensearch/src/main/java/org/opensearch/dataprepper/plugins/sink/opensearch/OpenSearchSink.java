/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.sink.opensearch;

import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.transport.TransportOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.plugin.PluginConfigObservable;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.AbstractSink;
import org.opensearch.dataprepper.model.sink.Sink;
import org.opensearch.dataprepper.model.sink.SinkContext;
import org.opensearch.dataprepper.plugins.common.opensearch.ServerlessNetworkPolicyUpdater;
import org.opensearch.dataprepper.plugins.common.opensearch.ServerlessNetworkPolicyUpdaterFactory;
import org.opensearch.dataprepper.plugins.common.opensearch.ServerlessOptionsFactory;
import org.opensearch.dataprepper.plugins.sink.opensearch.configuration.OpenSearchSinkConfig;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.ClusterSettingsParser;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.CustomDocumentBuilderFactory;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexManager;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexManagerFactory;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexTemplateAPIWrapper;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexTemplateAPIWrapperFactory;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.SemanticEnrichmentIndexManager;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexType;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.TemplateStrategy;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.ServerlessOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.function.Function;

@DataPrepperPlugin(name = "opensearch", pluginType = Sink.class, pluginConfigurationType = OpenSearchSinkConfig.class)
public class OpenSearchSink extends AbstractSink<Record<Event>> {
  public static final String BULKREQUEST_LATENCY = "bulkRequestLatency";
  public static final String BULKREQUEST_ERRORS = "bulkRequestErrors";
  public static final String INVALID_ACTION_ERRORS = "invalidActionErrors";
  public static final String BULKREQUEST_SIZE_BYTES = "bulkRequestSizeBytes";
  public static final String DYNAMIC_INDEX_DROPPED_EVENTS = "dynamicIndexDroppedEvents";
  public static final String INVALID_VERSION_EXPRESSION_DROPPED_EVENTS = "dynamicDocumentVersionDroppedEvents";

  private static final Logger LOG = LoggerFactory.getLogger(OpenSearchSink.class);
  private static final int INITIALIZE_RETRY_WAIT_TIME_MS = 5000;

  private final AwsCredentialsSupplier awsCredentialsSupplier;
  private final OpenSearchSinkConfiguration openSearchSinkConfig;
  private final IndexManagerFactory indexManagerFactory;
  private final IndexType indexType;
  private final PluginConfigObservable pluginConfigObservable;
  private final Ingester ingester;

  private final RestHighLevelClient restHighLevelClient;
  private final OpenSearchClient openSearchClient;
  private final OpenSearchClientRefresher openSearchClientRefresher;
  private IndexManager indexManager;
  private volatile boolean initialized;

  @DataPrepperPluginConstructor
  public OpenSearchSink(final PluginSetting pluginSetting,
                        final SinkContext sinkContext,
                        final ExpressionEvaluator expressionEvaluator,
                        final AwsCredentialsSupplier awsCredentialsSupplier,
                        final PipelineDescription pipelineDescription,
                        final PluginConfigObservable pluginConfigObservable,
                        final OpenSearchSinkConfig openSearchSinkConfiguration) {
    super(pluginSetting, Integer.MAX_VALUE, INITIALIZE_RETRY_WAIT_TIME_MS);
    this.awsCredentialsSupplier = awsCredentialsSupplier;
    this.pluginConfigObservable = pluginConfigObservable;

    final SinkContext resolvedSinkContext = sinkContext != null ? sinkContext :
            new SinkContext(null, Collections.emptyList(), Collections.emptyList(), Collections.emptyList());

    this.openSearchSinkConfig = OpenSearchSinkConfiguration.readOSConfig(openSearchSinkConfiguration, expressionEvaluator);
    this.indexType = openSearchSinkConfig.getIndexConfiguration().getIndexType();
    this.indexManagerFactory = new IndexManagerFactory(new ClusterSettingsParser());
    this.initialized = false;

    final ConnectionConfiguration connectionConfiguration = openSearchSinkConfig.getConnectionConfiguration();
    restHighLevelClient = connectionConfiguration.createClient(awsCredentialsSupplier);
    openSearchClient = connectionConfiguration.createOpenSearchClient(restHighLevelClient, awsCredentialsSupplier);
    final Function<ConnectionConfiguration, OpenSearchClient> clientFunction =
            (connConfig) -> {
      final RestHighLevelClient client = connConfig.createClient(awsCredentialsSupplier);
      return connConfig.createOpenSearchClient(client, awsCredentialsSupplier).withTransportOptions(
              TransportOptions.builder()
                      .setParameter("filter_path", "errors,took,items.*.error,items.*.status,items.*._index,items.*._id")
                      .build());
    };
    openSearchClientRefresher = new OpenSearchClientRefresher(
            pluginMetrics, connectionConfiguration, clientFunction);

    final String pipeline = pipelineDescription.getPipelineName();
    final EventActionResolver eventActionResolver = new EventActionResolver(
            openSearchSinkConfig.getIndexConfiguration().getAction(),
            openSearchSinkConfig.getIndexConfiguration().getActions(),
            expressionEvaluator);

    this.ingester = new BulkIngester(openSearchSinkConfig, expressionEvaluator, resolvedSinkContext,
            pluginMetrics, pipeline, eventActionResolver,
            openSearchClient, () -> openSearchClientRefresher.get(),
            this::getIndexManager, this::getFailurePipeline,
            new CustomDocumentBuilderFactory().create(this.indexType));
  }

  @Override
  public void doInitialize() {
    try {
        doInitializeInternal();
    } catch (IOException e) {
        LOG.warn("Failed to initialize OpenSearch sink, retrying: {} ", e.getMessage());
        this.shutdown();
    } catch (InvalidPluginConfigurationException e) {
        LOG.error("Failed to initialize OpenSearch sink due to a configuration error.", e);
        this.shutdown();
        throw new RuntimeException(e.getMessage(), e);
    } catch (IllegalArgumentException e) {
        LOG.error("Failed to initialize OpenSearch sink due to a configuration error.", e);
        this.shutdown();
        throw e;
    } catch (Exception e) {
        LOG.warn("Failed to initialize OpenSearch sink with a retryable exception. ", e);
        this.shutdown();
    }
  }

  private void doInitializeInternal() throws IOException {
    LOG.info("Initializing OpenSearch sink");

    pluginConfigObservable.addPluginConfigObserver(
            newOpenSearchSinkConfig -> openSearchClientRefresher.update((OpenSearchSinkConfig) newOpenSearchSinkConfig));

    final String configuredIndexAlias = openSearchSinkConfig.getIndexConfiguration().getIndexAlias();
    final IndexTemplateAPIWrapper indexTemplateAPIWrapper = IndexTemplateAPIWrapperFactory.getWrapper(
            openSearchSinkConfig.getIndexConfiguration(), openSearchClient);
    final TemplateStrategy templateStrategy = openSearchSinkConfig.getIndexConfiguration().getTemplateType()
            .createTemplateStrategy(indexTemplateAPIWrapper);
    indexManager = indexManagerFactory.getIndexManager(indexType, openSearchClient, restHighLevelClient,
            openSearchSinkConfig, templateStrategy, configuredIndexAlias);

    maybeUpdateServerlessNetworkPolicy();

    // Create index with semantic enrichment via AWS control plane (AOSS or managed domain) if configured.
    new SemanticEnrichmentIndexManager(awsCredentialsSupplier).maybeCreateIndex(
            openSearchSinkConfig.getConnectionConfiguration(),
            openSearchSinkConfig.getIndexConfiguration().getSemanticEnrichmentConfig(),
            openSearchSinkConfig.getIndexConfiguration().getSemanticEnrichmentResourceName(),
            configuredIndexAlias);

    indexManager.setupIndex();

    ingester.initialize();

    this.initialized = true;
    LOG.info("Initialized OpenSearch sink");
  }

  IndexManager getIndexManager() {
    return indexManager;
  }

  @Override
  public boolean isReady() {
    return initialized;
  }

  @Override
  public void doOutput(final Collection<Record<Event>> records) {
    ingester.output(records);
  }

  @Override
  public void shutdown() {
    super.shutdown();
    ingester.shutdown();
    if (restHighLevelClient != null) {
      try {
        restHighLevelClient.close();
      } catch (final IOException e) {
        throw new RuntimeException(e.getMessage(), e);
      }
    }
    if (openSearchClient != null) {
      openSearchClient.shutdown();
    }
  }

  private void maybeUpdateServerlessNetworkPolicy() {
    final Optional<ServerlessOptions> maybeServerlessOptions = ServerlessOptionsFactory.create(
        openSearchSinkConfig.getConnectionConfiguration());

    if (maybeServerlessOptions.isPresent()) {
      final ServerlessNetworkPolicyUpdater networkPolicyUpdater = ServerlessNetworkPolicyUpdaterFactory.create(
          awsCredentialsSupplier, openSearchSinkConfig.getConnectionConfiguration()
      );
      networkPolicyUpdater.updateNetworkPolicy(
          maybeServerlessOptions.get().getNetworkPolicyName(),
          maybeServerlessOptions.get().getCollectionName(),
          maybeServerlessOptions.get().getVpceId()
      );
    }
  }
}
