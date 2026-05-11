/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.prometheus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

final class PrometheusMetricUtils {

    private static final Logger LOG = LoggerFactory.getLogger(PrometheusMetricUtils.class);

    static final String AGGREGATION_TEMPORALITY_CUMULATIVE = "AGGREGATION_TEMPORALITY_CUMULATIVE";

    private static final String TOTAL_SUFFIX = "_total";
    private static final String CREATED_SUFFIX = "_created";
    private static final String SERVICE_NAME_LABEL = "service.name";
    private static final String SERVICE_NAME_UNDERSCORE_LABEL = "service_name";
    private static final String JOB_LABEL = "job";

    private PrometheusMetricUtils() {
    }

    static String extractServiceName(final Map<String, Object> attributes) {
        if (attributes.containsKey(SERVICE_NAME_LABEL)) {
            return (String) attributes.get(SERVICE_NAME_LABEL);
        }
        if (attributes.containsKey(SERVICE_NAME_UNDERSCORE_LABEL)) {
            return (String) attributes.get(SERVICE_NAME_UNDERSCORE_LABEL);
        }
        if (attributes.containsKey(JOB_LABEL)) {
            return (String) attributes.get(JOB_LABEL);
        }
        return "";
    }

    static String stripCounterSuffix(final String metricName) {
        if (metricName.endsWith(TOTAL_SUFFIX)) {
            return metricName.substring(0, metricName.length() - TOTAL_SUFFIX.length());
        }
        if (metricName.endsWith(CREATED_SUFFIX)) {
            return metricName.substring(0, metricName.length() - CREATED_SUFFIX.length());
        }
        return metricName;
    }

    static Double parseLeValue(final String leValue) {
        if (leValue == null) {
            return null;
        }
        if ("+Inf".equals(leValue)) {
            return Double.POSITIVE_INFINITY;
        }
        if ("-Inf".equals(leValue)) {
            return Double.NEGATIVE_INFINITY;
        }
        try {
            return Double.parseDouble(leValue);
        } catch (final NumberFormatException e) {
            LOG.warn("Skipping histogram bucket with unparseable le value: '{}'", leValue);
            return null;
        }
    }

    static Double parseQuantileValue(final String quantileValue) {
        if (quantileValue == null) {
            return null;
        }
        try {
            return Double.parseDouble(quantileValue);
        } catch (final NumberFormatException e) {
            LOG.warn("Skipping summary quantile with unparseable value: '{}'", quantileValue);
            return null;
        }
    }
}
