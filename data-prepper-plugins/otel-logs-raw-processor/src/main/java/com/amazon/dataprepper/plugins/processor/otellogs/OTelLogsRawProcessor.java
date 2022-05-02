/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.otellogs;

import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.logs.v1.InstrumentationLibraryLogs;
import io.opentelemetry.proto.logs.v1.LogRecord;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.logs.v1.ScopeLogs;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.log.JacksonOtelLog;
import org.opensearch.dataprepper.model.log.OpenTelemetryLog;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoCodec;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


@DataPrepperPlugin(name = "otel_logs_raw_processor", pluginType = Processor.class)
public class OTelLogsRawProcessor extends AbstractProcessor<Record<ExportLogsServiceRequest>, Record<? extends OpenTelemetryLog>> {


    @DataPrepperPluginConstructor
    public OTelLogsRawProcessor(PluginSetting pluginSetting) {
        super(pluginSetting);
    }

    @Override
    public Collection<Record<? extends OpenTelemetryLog>> doExecute(Collection<Record<ExportLogsServiceRequest>> records) {
        Collection<Record<? extends OpenTelemetryLog>> recordsOut = new ArrayList<>();
        for (Record<ExportLogsServiceRequest> ets : records) {
            for (ResourceLogs rs : ets.getData().getResourceLogsList()) {
                final String schemaUrl = rs.getSchemaUrl();
                final Map<String, Object> resourceAttributes = OTelProtoCodec.getResourceAttributes(rs.getResource());
                final String serviceName = OTelProtoCodec.getServiceName(rs.getResource()).orElse(null);

                for (InstrumentationLibraryLogs is : rs.getInstrumentationLibraryLogsList()) {
                    final Map<String, Object> ils = OTelProtoCodec.getInstrumentationLibraryAttributes(is.getInstrumentationLibrary());
                    recordsOut.addAll(processLogsList(is.getLogRecordsList(), serviceName, ils, resourceAttributes, schemaUrl));
                }

                for (ScopeLogs sl : rs.getScopeLogsList()) {
                    final Map<String, Object> ils = OTelProtoCodec.getInstrumentationScopeAttributes(sl.getScope());
                    recordsOut.addAll(processLogsList(sl.getLogRecordsList(), serviceName, ils, resourceAttributes, schemaUrl));
                }

            }
        }
        return recordsOut;
    }

    private Collection<? extends Record<? extends OpenTelemetryLog>> processLogsList(final List<LogRecord> logsList,
                                                                        final String serviceName,
                                                                        final Map<String, Object> ils,
                                                                        final Map<String, Object> resourceAttributes,
                                                                        final String schemaUrl) {
        return logsList.stream()
                .map(log -> JacksonOtelLog.builder()
                        .withTime(OTelProtoCodec.convertUnixNanosToISO8601(log.getTimeUnixNano()))
                        .withObservedTime(OTelProtoCodec.convertUnixNanosToISO8601(log.getObservedTimeUnixNano()))
                        .withServiceName(serviceName)
                        .withAttributes(OTelProtoCodec.mergeAllAttributes(
                                Arrays.asList(
                                        OTelProtoCodec.unpackKeyValueListLog(log.getAttributesList()),
                                        resourceAttributes,
                                        ils
                                )
                        ))
                        .withSchemaUrl(schemaUrl)
                        .withFlags(log.getFlags())
                        .withTraceId(OTelProtoCodec.convertByteStringToString(log.getTraceId()))
                        .withSpanId(OTelProtoCodec.convertByteStringToString(log.getSpanId()))
                        .withSeverityNumber(log.getSeverityNumberValue())
                        .withDroppedAttributesCount(log.getDroppedAttributesCount())
                        .withBody(OTelProtoCodec.convertAnyValue(log.getBody()))
                        .build())
                .map(Record::new)
                .collect(Collectors.toList());
    }

    @Override
    public void prepareForShutdown() {
    }

    @Override
    public boolean isReadyForShutdown() {
        return true;
    }

    @Override
    public void shutdown() {
    }
}
