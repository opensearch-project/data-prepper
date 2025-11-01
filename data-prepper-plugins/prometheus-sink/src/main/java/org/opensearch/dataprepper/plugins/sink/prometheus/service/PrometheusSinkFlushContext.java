/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.prometheus.service;

import org.opensearch.dataprepper.common.sink.SinkFlushContext;
import org.opensearch.dataprepper.plugins.sink.prometheus.PrometheusHttpSender;

public class PrometheusSinkFlushContext implements SinkFlushContext {
    private final PrometheusHttpSender httpSender;

    public PrometheusSinkFlushContext(final PrometheusHttpSender httpSender) {
        this.httpSender = httpSender;
    }

    public PrometheusHttpSender getHttpSender() {
        return httpSender;
    }
}
