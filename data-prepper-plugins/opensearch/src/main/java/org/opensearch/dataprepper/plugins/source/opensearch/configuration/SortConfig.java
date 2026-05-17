/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.opensearch.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;

public class SortConfig {

    @JsonProperty("name")
    @NotBlank(message = "sort field name must not be blank")
    private String name;

    @JsonProperty("order")
    private SortOrder order = SortOrder.ASCENDING;

    public String getName() {
        return name;
    }

    public SortOrder getOrder() {
        return order;
    }
}