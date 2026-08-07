/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.model.metric;

public class JacksonStandardGauge extends JacksonGauge {

    public JacksonStandardGauge(JacksonStandardGauge.Builder builder) {
        super(builder, false);
    }

    public static JacksonStandardGauge.Builder builder() {
        return new JacksonStandardGauge.Builder();
    }

    public static class Builder extends JacksonGauge.Builder {

        @Override
        public JacksonGauge build() {
            populateEvent(KIND.GAUGE.toString());
            return new JacksonStandardGauge(this);
        }
    }

}


