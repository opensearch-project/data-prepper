/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3;

import com.fasterxml.jackson.annotation.JsonProperty;
public class PredefinedObjectMetadata {
    @JsonProperty("number_of_objects")
    private String numberOfObjects;

    public String getNumberOfObjects() {
        return numberOfObjects;
    }

}
