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

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import software.amazon.kinesis.processor.RecordProcessorCheckpointer;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

@Builder
@Getter
@Setter
public class KinesisCheckpointerRecord {
    private RecordProcessorCheckpointer checkpointer;
    private ExtendedSequenceNumber extendedSequenceNumber;
    private boolean      readyToCheckpoint;
}
