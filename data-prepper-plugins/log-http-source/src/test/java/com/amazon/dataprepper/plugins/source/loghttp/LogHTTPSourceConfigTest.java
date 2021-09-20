/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper.plugins.source.loghttp;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class LogHTTPSourceConfigTest {
    private static final String PLUGIN_NAME = "log_http_source";

    @Test
    public void testDefault() {
        // TODO: write test logic
    }

    @Test
    public void testValidConfig() {
        // TODO: write test logic
    }

    private PluginSetting completePluginSettingForLogHTTPSource() {
        final Map<String, Object> settings = new HashMap<>();
        // TODO: add parameters
        return new PluginSetting(PLUGIN_NAME, settings);
    }
}