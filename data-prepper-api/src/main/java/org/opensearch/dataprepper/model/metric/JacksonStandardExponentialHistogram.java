/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.metric;

public class JacksonStandardExponentialHistogram extends JacksonExponentialHistogram {

    public JacksonStandardExponentialHistogram(JacksonStandardExponentialHistogram.Builder builder) {
        super(builder, false);
    }

    public static JacksonStandardExponentialHistogram.Builder builder() {
        return new JacksonStandardExponentialHistogram.Builder();
    }

    @Override
    public String toJsonString() {
        return getJsonNode().toString();
    }

    public static class Builder extends JacksonExponentialHistogram.Builder {

        @Override
        public JacksonExponentialHistogram build() {
            populateEvent(KIND.EXPONENTIAL_HISTOGRAM.toString());
            return new JacksonStandardExponentialHistogram(this);
        }
    }

}

