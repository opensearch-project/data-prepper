/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.pull_ingestion;

import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.SinkContext;
import org.opensearch.dataprepper.plugins.sink.opensearch.DocumentIdResolver;
import org.opensearch.dataprepper.plugins.sink.opensearch.EventActionResolver;
import org.opensearch.dataprepper.plugins.sink.opensearch.Ingester;
import org.opensearch.dataprepper.plugins.sink.opensearch.OpenSearchSinkConfiguration;
import org.opensearch.dataprepper.plugins.sink.opensearch.PullEngine;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.DocumentBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;

public class PullIngester implements Ingester {
    private static final Logger LOG = LoggerFactory.getLogger(PullIngester.class);

    private final PullEngine pullEngine;
    private final IndexRouter indexRouter;
    private final IndexShardProvider indexShardProvider;
    private final PullIngestionEnvelopeBuilder envelopeBuilder;
    private final DocumentIdResolver documentIdResolver;
    private final OpenSearchSinkConfiguration openSearchSinkConfig;
    private final ExpressionEvaluator expressionEvaluator;
    private final SinkContext sinkContext;
    private final EventActionResolver eventActionResolver;
    private final PullIngestionMetrics pullIngestionMetrics;

    private final String routingField;
    private final String routing;
    private final String versionExpression;
    private final String documentRootKey;

    public PullIngester(final PullEngine pullEngine,
                        final IndexRouter indexRouter,
                        final IndexShardProvider indexShardProvider,
                        final PullIngestionEnvelopeBuilder envelopeBuilder,
                        final DocumentIdResolver documentIdResolver,
                        final OpenSearchSinkConfiguration openSearchSinkConfig,
                        final ExpressionEvaluator expressionEvaluator,
                        final SinkContext sinkContext,
                        final EventActionResolver eventActionResolver,
                        final PullIngestionMetrics pullIngestionMetrics) {
        this.pullEngine = pullEngine;
        this.indexRouter = indexRouter;
        this.indexShardProvider = indexShardProvider;
        this.envelopeBuilder = envelopeBuilder;
        this.documentIdResolver = documentIdResolver;
        this.openSearchSinkConfig = openSearchSinkConfig;
        this.expressionEvaluator = expressionEvaluator;
        this.sinkContext = sinkContext;
        this.eventActionResolver = eventActionResolver;
        this.pullIngestionMetrics = pullIngestionMetrics;

        this.routingField = openSearchSinkConfig.getIndexConfiguration().getRoutingField();
        this.routing = openSearchSinkConfig.getIndexConfiguration().getRouting();
        this.versionExpression = openSearchSinkConfig.getIndexConfiguration().getVersionExpression();
        this.documentRootKey = openSearchSinkConfig.getIndexConfiguration().getDocumentRootKey();
    }

    @Override
    public void initialize() throws IOException {
        final String indexName = openSearchSinkConfig.getIndexConfiguration().getIndexAlias();
        indexRouter.initialize(indexName);
        final String topicName = indexShardProvider.getIngestionTopic(indexName);
        pullEngine.initialize(topicName, indexRouter.getNumberOfShards());
    }

    @Override
    public void output(final Collection<Record<Event>> records) {
        final String indexName = openSearchSinkConfig.getIndexConfiguration().getIndexAlias();

        for (final Record<Event> record : records) {
            final Event event = record.getData();

            try {
                final String docId = documentIdResolver.resolve(event)
                        .orElseThrow(() -> new IllegalStateException("document_id is required for pull-based ingestion"));
                final long version = resolveVersion(event);
                final String action = eventActionResolver.resolveAction(event, indexName);
                final String source = DocumentBuilder.build(event, documentRootKey,
                        sinkContext.getTagsTargetKey(), sinkContext.getIncludeKeys(), sinkContext.getExcludeKeys());

                final byte[] envelope = envelopeBuilder.build(docId, version, source, action);

                final String routingValue = resolveRouting(event, docId);
                final int partition = indexRouter.getShardForRouting(routingValue);

                pullEngine.write(partition, docId, envelope);
                pullIngestionMetrics.recordBytes(envelope.length);
                pullIngestionMetrics.incrementDocumentsSucceeded();
                event.getEventHandle().release(true);
            } catch (final Exception e) {
                LOG.error("Failed to write event to pull engine", e);
                pullIngestionMetrics.incrementDocumentsFailed();
                event.getEventHandle().release(false);
            }
        }

        pullIngestionMetrics.recordLatency(pullEngine::flush);
    }

    @Override
    public void shutdown() {
        pullEngine.shutdown();
    }

    private long resolveVersion(final Event event) {
        if (versionExpression != null) {
            final String evaluated = event.formatString(versionExpression, expressionEvaluator);
            try {
                return Long.parseLong(evaluated);
            } catch (final NumberFormatException e) {
                throw new RuntimeException(String.format(
                        "document_version expression '%s' evaluated to '%s' which is not a valid long",
                        versionExpression, evaluated), e);
            }
        }
        throw new IllegalStateException("document_version is required for pull-based ingestion");
    }

    private String resolveRouting(final Event event, final String docId) {
        if (routingField != null) {
            final String value = event.get(routingField, String.class);
            if (value != null) {
                return value;
            }
        }
        if (routing != null) {
            try {
                return event.formatString(routing, expressionEvaluator);
            } catch (final Exception e) {
                LOG.warn("Unable to resolve routing expression '{}', falling back to document ID", routing, e);
            }
        }
        return docId;
    }
}
