/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.rds.utils;

import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.metrics.PluginMetrics;

public class RdsSourceAggregateMetrics {
    private static final String RDS_SOURCE = "rds";

    private static final String RDS_SOURCE_STREAM_5XX_EXCEPTIONS = "stream5xxErrors";
    private static final String RDS_SOURCE_STREAM_4XX_EXCEPTIONS = "stream4xxErrors";
    private static final String RDS_SOURCE_STREAM_API_INVOCATIONS = "streamApiInvocations";
    private static final String RDS_SOURCE_EXPORT_5XX_ERRORS = "export5xxErrors";
    private static final String RDS_SOURCE_EXPORT_4XX_ERRORS = "export4xxErrors";
    private static final String RDS_SOURCE_EXPORT_API_INVOCATIONS = "exportApiInvocations";
    private static final String RDS_SOURCE_EXPORT_PARTITION_QUERY_COUNT = "exportPartitionQueryCount";
    private static final String RDS_SOURCE_STREAM_AUTH_ERRORS = "streamAuthErrors";
    private static final String RDS_SOURCE_STREAM_SERVER_NOT_FOUND_ERRORS = "streamServerNotFoundErrors";
    private static final String RDS_SOURCE_STREAM_REPLICATION_NOT_ENABLED_ERRORS = "streamReplicationNotEnabledErrors";
    private static final String RDS_SOURCE_STREAM_ACCESS_DENIED_ERRORS = "streamAccessDeniedErrors";

    private final PluginMetrics pluginMetrics;
    private final Counter stream5xxErrors;
    private final Counter stream4xxErrors;
    private final Counter streamApiInvocations;
    private final Counter export5xxErrors;
    private final Counter export4xxErrors;
    private final Counter exportApiInvocations;
    private final Counter exportPartitionQueryCount;
    private final Counter streamAuthErrors;
    private final Counter streamServerNotFoundErrors;
    private final Counter streamReplicationNotEnabledErrors;
    private final Counter streamAccessDeniedErrors;

    public RdsSourceAggregateMetrics() {
        this.pluginMetrics = PluginMetrics.fromPrefix(RDS_SOURCE);
        this.stream5xxErrors = pluginMetrics.counter(RDS_SOURCE_STREAM_5XX_EXCEPTIONS);
        this.stream4xxErrors = pluginMetrics.counter(RDS_SOURCE_STREAM_4XX_EXCEPTIONS);
        this.streamApiInvocations = pluginMetrics.counter(RDS_SOURCE_STREAM_API_INVOCATIONS);
        this.export5xxErrors = pluginMetrics.counter(RDS_SOURCE_EXPORT_5XX_ERRORS);
        this.export4xxErrors = pluginMetrics.counter(RDS_SOURCE_EXPORT_4XX_ERRORS);
        this.exportApiInvocations = pluginMetrics.counter(RDS_SOURCE_EXPORT_API_INVOCATIONS);
        this.exportPartitionQueryCount = pluginMetrics.counter(RDS_SOURCE_EXPORT_PARTITION_QUERY_COUNT);

        // More granular error metrics
        this.streamAuthErrors = pluginMetrics.counter(RDS_SOURCE_STREAM_AUTH_ERRORS);
        this.streamServerNotFoundErrors = pluginMetrics.counter(RDS_SOURCE_STREAM_SERVER_NOT_FOUND_ERRORS);
        this.streamReplicationNotEnabledErrors = pluginMetrics.counter(RDS_SOURCE_STREAM_REPLICATION_NOT_ENABLED_ERRORS);
        this.streamAccessDeniedErrors = pluginMetrics.counter(RDS_SOURCE_STREAM_ACCESS_DENIED_ERRORS);
    }

    public Counter getStream5xxErrors() {
        return stream5xxErrors;
    }

    public Counter getStream4xxErrors() {
        return stream4xxErrors;
    }

    public Counter getStreamApiInvocations() {
        return streamApiInvocations;
    }

    public Counter getExport5xxErrors() {
        return export5xxErrors;
    }

    public Counter getExport4xxErrors() {
        return export4xxErrors;
    }

    public Counter getExportApiInvocations() {
        return exportApiInvocations;
    }

    public Counter getExportPartitionQueryCount() {
        return exportPartitionQueryCount;
    }

    public Counter getStreamAuthErrors() {
        return streamAuthErrors;
    }

    public Counter getStreamServerNotFoundErrors() {
        return streamServerNotFoundErrors;
    }

    public Counter getStreamReplicationNotEnabledErrors() {
        return streamReplicationNotEnabledErrors;
    }

    public Counter getStreamAccessDeniedErrors() {
        return streamAccessDeniedErrors;
    }
}
