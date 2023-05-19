/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch;

import org.opensearch.client.json.JsonpDeserializer;

public class PITResponse  {

    public static JsonpDeserializer<PITResponse> PARSER;

    public String pitId ;

    public String getPitId() {
        return pitId;
    }

    public void setPitId(String pitId) {
        this.pitId = pitId;
    }

}
