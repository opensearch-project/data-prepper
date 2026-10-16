/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.obfuscation.action;

import java.util.List;
import org.opensearch.dataprepper.model.pattern.Pattern;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;


/**
 * Interface represents a specific action to be taken for obfuscation.
 */
public interface ObfuscationAction {

    /**
     * Obfuscate the source string based on a list of patterns.
     * If the list is empty, the whole field will be obfuscated.
     *
     * @param source   source string
     * @param patterns a list of patterns to match
     * @param record  raw record
     * @return obfuscated string
     */
    String obfuscate(String source, List<Pattern> patterns, Record<Event> record);
}
