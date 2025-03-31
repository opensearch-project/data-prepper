/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.ml_inference.processor.common;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

public interface MLBatchJobCreator {
    void createMLBatchJob(List<Record<Event>> inputRecords, List<Record<Event>> resultRecords);

    default String generateJobName() {
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS"));
        String jobName = "OSI-batch-job-" + timestamp;
        return jobName.replaceAll("[^a-zA-Z0-9-]", "").substring(0, Math.min(63, jobName.length()));
    }
}
