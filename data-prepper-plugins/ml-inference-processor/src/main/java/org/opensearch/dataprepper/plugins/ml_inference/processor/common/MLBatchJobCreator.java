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
import java.util.regex.Pattern;

public interface MLBatchJobCreator {
    void createMLBatchJob(List<Record<Event>> inputRecords, List<Record<Event>> resultRecords);

    // Define DateTimeFormatter as a static constant to reuse it
    DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS");
    Pattern JOB_NAME_PATTERN = Pattern.compile("[^a-zA-Z0-9-]");

    default String generateJobName() {
        String timestamp = LocalDateTime.now().format(TIMESTAMP_FORMATTER);
        String jobName = "DataPrepper-batch-job-" + timestamp;

        jobName = JOB_NAME_PATTERN.matcher(jobName).replaceAll("");
        return jobName.substring(0, Math.min(63, jobName.length()));
    }
}
