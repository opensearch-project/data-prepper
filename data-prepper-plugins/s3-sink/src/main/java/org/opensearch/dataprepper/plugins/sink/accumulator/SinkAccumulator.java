/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.accumulator;

/**
 * {@link SinkAccumulator} Accumulate buffer records
 */
public interface SinkAccumulator {

	void doAccumulate();
}
