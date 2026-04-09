/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import org.opensearch.dataprepper.model.event.Event;

import java.util.List;

/**
 * Converts a Data Prepper {@link Event} into one or more JSON document strings
 * suitable for indexing into OpenSearch. Implementations handle index-type-specific
 * document transformations such as TSDB metric expansion.
 */
public interface CustomDocumentBuilder {
    List<String> buildDocuments(Event event);
}