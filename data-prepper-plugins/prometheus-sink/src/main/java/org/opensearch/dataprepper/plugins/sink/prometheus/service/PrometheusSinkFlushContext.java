 /*
  * Copyright OpenSearch Contributors
  * SPDX-License-Identifier: Apache-2.0
  *
  * The OpenSearch Contributors require contributions made to
  * this file be licensed under the Apache-2.0 license or a
  * compatible open source license.
  *
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
