/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3;

import java.util.HashMap;
import java.util.Map;

public class ObjectMetadata {
    private Map<String, String> metadata;
    private ObjectMetadataConfig objectMetadataConfig;

    public ObjectMetadata(final ObjectMetadataConfig objectMetadataConfig) {
        this.objectMetadataConfig = objectMetadataConfig;
        this.metadata = new HashMap<String, String>();
    }
   
    public void setEventCount(int eventCount) {
       String numberOfEventsKey = objectMetadataConfig.getNumberOfEventsKey();
       if (numberOfEventsKey != null) {
           metadata.put(numberOfEventsKey, Integer.toString(eventCount));
       }
    }

    public Map<String, String> get() {
        return metadata;
    }
}


