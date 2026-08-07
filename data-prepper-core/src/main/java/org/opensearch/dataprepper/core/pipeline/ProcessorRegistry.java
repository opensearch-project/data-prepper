/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.core.pipeline;

import org.opensearch.dataprepper.model.processor.Processor;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@SuppressWarnings({"rawtypes"})
public class ProcessorRegistry implements ProcessorProvider {
    private volatile List<Processor> processors;

    public ProcessorRegistry(List<Processor> initialProcessors) {
        this.processors = new ArrayList<>(initialProcessors);
    }

    // Atomic swap of entire processor list
    public void swapProcessors(List<Processor> newProcessors) {
        Objects.requireNonNull(newProcessors, "New processors list cannot be null");
        this.processors = new ArrayList<>(newProcessors);
    }

    // Get current processors for execution
    @Override
    public List<Processor> getProcessors() {
        return processors;
    }
}
