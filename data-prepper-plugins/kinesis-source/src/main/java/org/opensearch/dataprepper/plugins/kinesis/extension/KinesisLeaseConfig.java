/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.kinesis.extension;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;

@Getter
public class KinesisLeaseConfig {
    @JsonProperty("lease_coordination")
    private KinesisLeaseCoordinationTableConfig leaseCoordinationTable;

    @JsonProperty("pipeline_identifier")
    private String pipelineIdentifier;
}
