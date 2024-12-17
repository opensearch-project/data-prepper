/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.kinesis.source.processor;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;

@Builder(setterPrefix = "with")
@Getter
@AllArgsConstructor
public class KinesisInputOutputRecord {
    private Record<Event> dataPrepperRecord;
    private long incomingRecordSizeBytes;
}
