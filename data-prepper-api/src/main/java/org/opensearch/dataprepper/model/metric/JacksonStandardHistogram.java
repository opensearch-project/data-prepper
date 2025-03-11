/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.metric;

public class JacksonStandardHistogram extends JacksonHistogram {

    public JacksonStandardHistogram(JacksonHistogram.Builder builder) {
        super(builder, false);
    }

}
