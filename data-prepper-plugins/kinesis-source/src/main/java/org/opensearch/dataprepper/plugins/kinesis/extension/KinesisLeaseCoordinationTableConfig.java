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
import lombok.NonNull;
import software.amazon.awssdk.regions.Region;

@Getter
public class KinesisLeaseCoordinationTableConfig {

    @JsonProperty("table_name")
    @NonNull
    private String tableName;

    @JsonProperty("region")
    @NonNull
    private String region;

    public Region getAwsRegion() {
        return Region.of(region);
    }
}
