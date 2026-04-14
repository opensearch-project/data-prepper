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
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotBlank;

import java.util.Set;

public class SortConfig {

    private static final Set<String> VALID_ORDERS = Set.of("ascending", "descending");

    @JsonProperty("name")
    @NotBlank(message = "sort field name must not be blank")
    private String name;

    @JsonProperty("order")
    private String order = "ascending";

    public String getName() {
        return name;
    }

    public String getOrder() {
        return order;
    }

    @AssertTrue(message = "sort order must be one of [ 'ascending', 'descending' ]")
    boolean isOrderValid() {
        return order == null || VALID_ORDERS.contains(order);
    }
}
