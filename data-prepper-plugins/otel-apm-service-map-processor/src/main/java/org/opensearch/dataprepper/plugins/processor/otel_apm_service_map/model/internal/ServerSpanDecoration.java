/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.internal;

import lombok.Getter;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;

/**
 * Decoration for SERVER spans containing pre-computed relationship data
 * (groupByAttributes are read directly from SpanStateData to avoid duplication)
 */
@Getter
public class ServerSpanDecoration implements Serializable {
    private final Collection<SpanStateData> clientDescendants;

    public ServerSpanDecoration(final Collection<SpanStateData> clientDescendants) {
        this.clientDescendants = clientDescendants != null ? Collections.unmodifiableCollection(clientDescendants) : Collections.emptyList();
    }
}
