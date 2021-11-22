/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.model.prepper;

import com.amazon.dataprepper.model.processor.Processor;
import com.amazon.dataprepper.model.record.Record;

/**
 * @deprecated as of 1.2, replace by {@link Processor}
 * Prepper interface. These are intermediary processing units using which users can filter,
 * transform and enrich the records into desired format before publishing to the sink.
 *
 */
@Deprecated
public interface Prepper<InputRecord extends Record<?>, OutputRecord extends Record<?>> extends Processor<InputRecord, OutputRecord> {
}
