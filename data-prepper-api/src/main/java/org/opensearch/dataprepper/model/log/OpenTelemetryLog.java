/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.log;

import java.util.Map;

/**
 * An OpenTelemetry log event in Data Prepper which represents a single log data point.
 *
 * @see <a href="https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/logs/v1/logs.proto">
 *     OpenTelemetry Protobuf Logs Spec</a>
 * @since 1.5
 */
public interface OpenTelemetryLog extends Log {

    /**
     * Gets the serviceName of this log.
     *
     * @return the ServiceName
     * @since 1.5
     */
    String getServiceName();

    /**
     * Gets ISO8601 representation of the observed time of the log event.
     *
     * @return the event time
     * @since 1.5
     */
    String getObservedTime();

    /**
     * Gets ISO8601 representation of the time of the log event.
     *
     * @return the event time
     * @since 1.5
     */
    String getTime();

    /**
     * Gets a String description of the log's attributes.
     *
     * @return the attributes
     * @since 1.5
     */
    Map<String, Object> getAttributes();

    /**
     * Gets the schema url of this log event.
     *
     * @return the schemaUrl
     * @since 1.5
     */
    String getSchemaUrl();

    /**
     * Gets the associated flags for this log event.
     *
     * @return the flags encoded as Integer
     * @since 1.5
     */
    Integer getFlags();

    /**
     * Gets the span id of this log event.
     *
     * @return the span id
     * @since 1.5
     */
    String getSpanId();

    /**
     * Gets the trace id of this log event.
     *
     * @return the trace id
     * @since 1.5
     */
    String getTraceId();

    /**
     * Gets the severity number of this log event.
     *
     * @return the severity number encoded as Integer
     * @since 1.5
     */
    Integer getSeverityNumber();

    /**
     * Gets the dropped attributes count of this log event.
     *
     * @return the dropped attributes count as Integer
     * @since 1.5
     */
    Integer getDroppedAttributesCount();

    /**
     * Gets the body of this log event.
     *
     * @return the body
     * @since 1.5
     */
    Object getBody();
}
