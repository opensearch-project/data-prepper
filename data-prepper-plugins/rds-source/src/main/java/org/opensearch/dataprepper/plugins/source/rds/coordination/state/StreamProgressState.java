/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.coordination.state;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.opensearch.dataprepper.plugins.source.rds.model.BinlogCoordinate;

public class StreamProgressState {

    @JsonProperty("currentPosition")
    private BinlogCoordinate currentPosition;

    @JsonProperty("waitForExport")
    private boolean waitForExport = false;

    public BinlogCoordinate getCurrentPosition() {
        return currentPosition;
    }

    public void setCurrentPosition(BinlogCoordinate currentPosition) {
        this.currentPosition = currentPosition;
    }

    public boolean shouldWaitForExport() {
        return waitForExport;
    }

    public void setWaitForExport(boolean waitForExport) {
        this.waitForExport = waitForExport;
    }
}
