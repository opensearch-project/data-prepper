/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.s3;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

public class S3SourceProgressState {

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Long lastTimeObjectsFound;

    @JsonCreator
    public S3SourceProgressState(@JsonProperty("last_time_objects_found") final Long lastTimeObjectsFound) {
        this.lastTimeObjectsFound = lastTimeObjectsFound;
    }

    public Long getLastTimeObjectsFound() {
        return lastTimeObjectsFound;
    }

    public void setLastTimeObjectsFound(final Long lastTimeObjectsFound) {
        this.lastTimeObjectsFound = lastTimeObjectsFound;
    }
}
