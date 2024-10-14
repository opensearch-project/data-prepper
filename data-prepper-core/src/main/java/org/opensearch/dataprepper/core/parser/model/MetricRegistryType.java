/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.parser.model;

public enum MetricRegistryType {
    // TODO: capitalize enum values
    Prometheus,
    CloudWatch,
    EmbeddedMetricsFormat;
}