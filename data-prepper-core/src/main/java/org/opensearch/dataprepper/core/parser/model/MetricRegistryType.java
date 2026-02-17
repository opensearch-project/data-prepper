/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.core.parser.model;

public enum MetricRegistryType {
    // TODO: capitalize enum values
    Prometheus,
    CloudWatch,
    EmbeddedMetricsFormat;
}