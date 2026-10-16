/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.ml_inference.processor.common;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;

import java.util.Collection;
import java.util.List;

public interface MLActionExecutor {
    default void prepareExecution(List<Record<Event>> resultRecords) {}

    Collection<Record<Event>> execute(List<Record<Event>> filteredRecords, List<Record<Event>> resultRecords);

    default void prepareForShutdown() {}

    default boolean isReadyForShutdown() { return true; }

    default void shutdown() {}
}
