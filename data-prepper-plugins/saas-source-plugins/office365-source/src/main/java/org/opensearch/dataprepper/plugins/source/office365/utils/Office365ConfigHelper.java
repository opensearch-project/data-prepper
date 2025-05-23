/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.office365.utils;

import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.plugins.source.office365.Office365SourceConfig;

public class Office365ConfigHelper {
    public static void validateConfig(Office365SourceConfig config) {
        if (config == null) {
            throw new InvalidPluginConfigurationException("Office365 source config cannot be null");
        }
    }
}