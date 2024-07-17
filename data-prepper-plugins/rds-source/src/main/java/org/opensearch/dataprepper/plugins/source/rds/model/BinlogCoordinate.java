/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class BinlogCoordinate {

    @JsonProperty("binlogFilename")
    private final String binlogFilename;

    @JsonProperty("binlogPosition")
    private final long binlogPosition;

    @JsonCreator
    public BinlogCoordinate(@JsonProperty("binlogFilename") String binlogFilename,
                            @JsonProperty("binlogPosition") long binlogPosition) {
        this.binlogFilename = binlogFilename;
        this.binlogPosition = binlogPosition;
    }

    public String getBinlogFilename() {
        return binlogFilename;
    }

    public long getBinlogPosition() {
        return binlogPosition;
    }
}
