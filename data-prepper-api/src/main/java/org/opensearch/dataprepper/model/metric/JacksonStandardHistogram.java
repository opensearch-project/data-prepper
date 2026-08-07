/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.model.metric;

public class JacksonStandardHistogram extends JacksonHistogram {

    public JacksonStandardHistogram(JacksonStandardHistogram.Builder builder) {
        super(builder, false);
    }

    public static JacksonStandardHistogram.Builder builder() {
        return new JacksonStandardHistogram.Builder();
    }

    public static class Builder extends JacksonHistogram.Builder {

        @Override
        public JacksonHistogram build() {
            populateEvent(KIND.HISTOGRAM.toString());
            return new JacksonStandardHistogram(this);
        }
    }

}
