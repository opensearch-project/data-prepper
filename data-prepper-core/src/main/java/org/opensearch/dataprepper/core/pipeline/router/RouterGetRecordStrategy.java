/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.pipeline.router;

import org.opensearch.dataprepper.model.record.Record;
import java.util.Collection;

public interface RouterGetRecordStrategy {
    public Record getRecord(Record record);

    public Collection<Record> getAllRecords(final Collection<Record> allRecords);
}
