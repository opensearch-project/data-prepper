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
    private PredefinedObjectMetadata predefinedObjectMetadata;

    public ObjectMetadata(final Object objectMetadataConfig) {
        if (objectMetadataConfig instanceof ObjectMetadataConfig) {
            this.objectMetadataConfig = (ObjectMetadataConfig)objectMetadataConfig;
        } else { // instanceof PredefinedObjectMetadata
            this.predefinedObjectMetadata = (PredefinedObjectMetadata)objectMetadataConfig;
        }
        this.metadata = new HashMap<String, String>();
    }
   
    public void setEventCount(final int eventCount) {
        String numberOfEventsKey = null;
        if (objectMetadataConfig != null) {
            numberOfEventsKey = objectMetadataConfig.getNumberOfEventsKey();
        } else if (predefinedObjectMetadata != null) {
            numberOfEventsKey = predefinedObjectMetadata.getNumberOfObjects();
        }
        if (numberOfEventsKey != null) {
            metadata.put(numberOfEventsKey, Integer.toString(eventCount));
        }
    }

    public Map<String, String> get() {
        return metadata;
    }

}


