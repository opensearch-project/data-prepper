/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch.model;

import org.opensearch.client.json.JsonpDeserializer;

public class PitResponse {

    private static JsonpDeserializer<PitResponse> PARSER;

    private String pitId ;

    public String getPitId() {
        return pitId;
    }

    public void setPitId(String pitId) {
        this.pitId = pitId;
    }

}
