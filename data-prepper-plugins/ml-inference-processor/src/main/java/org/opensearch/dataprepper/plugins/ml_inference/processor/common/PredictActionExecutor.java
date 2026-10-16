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

public class PredictActionExecutor implements MLActionExecutor {

    private final ModelSyncInferenceExecutor modelSyncInferenceExecutor;

    public PredictActionExecutor(final ModelSyncInferenceExecutor modelSyncInferenceExecutor) {
        this.modelSyncInferenceExecutor = modelSyncInferenceExecutor;
    }

    @Override
    public Collection<Record<Event>> execute(final List<Record<Event>> filteredRecords,
                                              final List<Record<Event>> resultRecords) {
        resultRecords.addAll(modelSyncInferenceExecutor.execute(filteredRecords));
        return resultRecords;
    }
}
