/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.mutateevent;

import jakarta.validation.constraints.NotEmpty;

public class DeleteProcessorConfig {
    @NotEmpty
    private String key;

    public String getKey() {
        return key;
    }
}
