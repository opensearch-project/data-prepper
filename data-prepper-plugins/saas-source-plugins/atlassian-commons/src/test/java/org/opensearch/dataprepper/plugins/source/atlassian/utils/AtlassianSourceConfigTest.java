/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */
package org.opensearch.dataprepper.plugins.source.atlassian.utils;

import org.opensearch.dataprepper.plugins.source.atlassian.AtlassianSourceConfig;

public class AtlassianSourceConfigTest extends AtlassianSourceConfig {
    @Override
    public String getOauth2UrlContext() {
        return "test";
    }
}
