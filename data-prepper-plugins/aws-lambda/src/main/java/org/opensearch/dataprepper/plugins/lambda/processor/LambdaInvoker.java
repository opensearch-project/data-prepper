/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.lambda.processor;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;

import java.util.Collection;
import java.util.List;

/**
 * Interface for Lambda invocation strategies.
 * Implementations handle different invocation types (streaming vs synchronous).
 */
public interface LambdaInvoker {
    /**
     * Invokes Lambda function with the provided records.
     *
     * @param recordsToLambda Records to send to Lambda
     * @param resultRecords Existing result records to append to
     * @return Collection of processed records
     */
    Collection<Record<Event>> invoke(List<Record<Event>> recordsToLambda, List<Record<Event>> resultRecords);
}
