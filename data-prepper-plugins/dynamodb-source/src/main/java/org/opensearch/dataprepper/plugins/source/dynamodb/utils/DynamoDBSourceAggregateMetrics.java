/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.utils;

import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.metrics.PluginMetrics;

public class DynamoDBSourceAggregateMetrics {

    private static final String DYNAMO_DB = "dynamodb";

    private static final String DDB_STREAM_5XX_EXCEPTIONS = "stream5xxErrors";
    private static final String DDB_STREAM_4XX_EXCEPTIONS = "stream4xxErrors";
    private static final String DDB_STREAM_API_INVOCATIONS = "streamApiInvocations";
    private static final String DDB_EXPORT_5XX_ERRORS = "export5xxErrors";
    private static final String DDB_EXPORT_4XX_ERRORS = "export4xxErrors";
    private static final String DDB_EXPORT_API_INVOCATIONS = "exportApiInvocations";



    private final PluginMetrics pluginMetrics;

    private final Counter stream5xxErrors;
    private final Counter stream4xxErrors;
    private final Counter streamApiInvocations;
    private final Counter export5xxErrors;
    private final Counter export4xxErrors;
    private final Counter exportApiInvocations;

    public DynamoDBSourceAggregateMetrics() {
        this.pluginMetrics = PluginMetrics.fromPrefix(DYNAMO_DB);
        this.stream5xxErrors = pluginMetrics.counter(DDB_STREAM_5XX_EXCEPTIONS);
        this.stream4xxErrors = pluginMetrics.counter(DDB_STREAM_4XX_EXCEPTIONS);
        this.streamApiInvocations = pluginMetrics.counter(DDB_STREAM_API_INVOCATIONS);
        this.export5xxErrors = pluginMetrics.counter(DDB_EXPORT_5XX_ERRORS);
        this.export4xxErrors = pluginMetrics.counter(DDB_EXPORT_4XX_ERRORS);
        this.exportApiInvocations = pluginMetrics.counter(DDB_EXPORT_API_INVOCATIONS);
    }

    public Counter getStream5xxErrors() {
        return stream5xxErrors;
    }

    public Counter getStream4xxErrors() { return stream4xxErrors; }

    public Counter getStreamApiInvocations() { return streamApiInvocations; }

    public Counter getExport5xxErrors() {
        return export5xxErrors;
    }

    public Counter getExport4xxErrors() { return export4xxErrors; }

    public Counter getExportApiInvocations() { return exportApiInvocations; }
}
