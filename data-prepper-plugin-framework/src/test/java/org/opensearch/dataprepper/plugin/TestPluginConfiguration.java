/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugin;

import jakarta.validation.constraints.NotNull;

public class TestPluginConfiguration {
    @NotNull
    private String requiredString;

    private String optionalString;

    public String getRequiredString() {
        return requiredString;
    }

    public void setRequiredString(final String requiredString) {
        this.requiredString = requiredString;
    }

    public String getOptionalString() {
        return optionalString;
    }

    public void setOptionalString(final String optionalString) {
        this.optionalString = optionalString;
    }
}
