/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.metric;

import org.opensearch.dataprepper.model.event.Event;

import java.util.List;
import java.util.Map;

/**
 * A metric event in Data Prepper represents a single metric data point. Every metric data point has a {@link KIND}.
 * Each Kind - also known as aggregation type - is a direct representation of one of the OpenTelemetry Metrics type.
 * @see <a href="https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/metrics/v1/metrics.proto">OTel Metrics Spec</a>
 *
 * @since 1.4
 */
public interface Metric extends Event {

    /**
     * The Kind of the event,
     * @see <a href="https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/metrics/datamodel.md">
     *     The OpenTelemetry Data Model Spec</a>
     */
    enum KIND {GAUGE, HISTOGRAM, EXPONENTIAL_HISTOGRAM, SUM, SUMMARY}

    /**
     * Gets the serviceName of this metric.
     *
     * @return the ServiceName
     * @since 1.4
     */
    String getServiceName();

    /**
     * Gets a name of the metric.
     *
     * @return the name
     * @since 1.4
     */
    String getName();

    /**
     * Gets a String description of the metric's operation.
     *
     * @return the name
     * @since 1.4
     */
    String getDescription();

    /**
     * Gets the unit in which the metric value is reported.
     *
     * @return the unit
     * @since 1.4
     */
    String getUnit();

    /**
     * Gets the kind of this metric. See {@link KIND}
     *
     * @return the kind of the metric
     * @since 1.4
     */
    String getKind();

    /**
     * Gets ISO8601 representation of the start time of the metric even.t
     *
     * @return the start time
     * @since 1.4
     */
    String getStartTime();

    /**
     * Gets ISO8601 representation of the time of the metric event.
     *
     * @return the event time
     * @since 1.4
     */
    String getTime();

    /**
     * Gets a collection of key-value pairs related to the metric event.
     *
     * @return A map of attributes
     * @since 1.4
     */
    Map<String, Object> getAttributes();

    /**
     * Gets the schema url of this metric.
     *
     * @return the schemaUrl
     * @since 1.4
     */
    String getSchemaUrl();

    /**
     * Gets the associated exemplars for this metric event.
     *
     * @return the exemplars
     * @since 1.4
     */
    List<? extends Exemplar> getExemplars();


    /**
     * Gets the associated flags for this metric event.
     *
     * @return the flags encoded as Integer
     * @since 1.4
     */
    Integer getFlags();

    /**
     * Gets the scope of this log event.
     *
     * @return the scope
     * @since 2.11
     */
    Map<String, Object> getScope();

    /**
     * Gets the resource of this log event.
     *
     * @return the resource
     * @since 2.11
     */
    Map<String, Object> getResource();

}
