/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.sink.opensearch;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;

import java.io.IOException;
import java.util.Collection;

/**
 * An interface for a mechanism to ingest data into OpenSearch.
 */
public interface Ingester {
    void initialize() throws IOException;

    void output(Collection<Record<Event>> records);

    void shutdown();
}
