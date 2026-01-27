/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.processor.model.internal;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

/**
 * Decoration for CLIENT spans containing pre-computed relationship data
 * (groupByAttributes are read directly from SpanStateData to avoid duplication)
 */
public class ClientSpanDecoration implements Serializable {
    public final String parentServerOperationName;
    public final String remoteEnvironment;
    public final String remoteService;
    public final String remoteOperation;
    public final Map<String, String> remoteGroupByAttributes;

    public ClientSpanDecoration(final String parentServerOperationName,
                               final String remoteEnvironment,
                               final String remoteService,
                               final String remoteOperation,
                               final Map<String, String> remoteGroupByAttributes) {
        this.parentServerOperationName = parentServerOperationName;
        this.remoteEnvironment = remoteEnvironment;
        this.remoteService = remoteService;
        this.remoteOperation = remoteOperation;
        this.remoteGroupByAttributes = remoteGroupByAttributes != null ? remoteGroupByAttributes : Collections.emptyMap();
    }
}
