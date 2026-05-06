/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.schemas;

public class JsonSchemaConverterConfig {
    private final boolean useDefinitions;

    public JsonSchemaConverterConfig(final boolean useDefinitions) {
        this.useDefinitions = useDefinitions;
    }

    public static JsonSchemaConverterConfig defaultConfig() {
        return new JsonSchemaConverterConfig(false);
    }

    public boolean isUseDefinitions() {
        return useDefinitions;
    }
}
