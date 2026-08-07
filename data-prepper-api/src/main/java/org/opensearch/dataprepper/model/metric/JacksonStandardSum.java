/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.model.metric;

public class JacksonStandardSum extends JacksonSum {

    public JacksonStandardSum(JacksonStandardSum.Builder builder) {
        super(builder, false);
    }

    public static JacksonStandardSum.Builder builder() {
        return new JacksonStandardSum.Builder();
    }

    public static class Builder extends JacksonSum.Builder {

        @Override
        public JacksonSum build() {
            populateEvent(KIND.SUM.toString());
            return new JacksonStandardSum(this);
        }
    }

}

