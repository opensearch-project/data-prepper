/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.model.metric;

public class JacksonStandardSummary extends JacksonSummary {

    public JacksonStandardSummary(JacksonStandardSummary.Builder builder) {
        super(builder, false);
    }

    public static JacksonStandardSummary.Builder builder() {
        return new JacksonStandardSummary.Builder();
    }

    public static class Builder extends JacksonSummary.Builder {

        @Override
        public JacksonSummary build() {
            populateEvent(KIND.SUMMARY.toString());
            return new JacksonStandardSummary(this);
        }
    }

}


