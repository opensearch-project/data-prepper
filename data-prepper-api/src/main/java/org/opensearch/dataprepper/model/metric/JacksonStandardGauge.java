/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
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


