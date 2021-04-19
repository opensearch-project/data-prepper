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

package com.amazon.dataprepper.plugins;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.source.Source;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("rawtypes")
public class PluginFactoryTests {
    public static final PluginSetting NON_EXISTENT_EMPTY_CONFIGURATION = new PluginSetting("does-not-exists", new HashMap<>());

    @Test
    public void testNoMandatoryConstructor() {
        final PluginSetting testPluginSettings = new PluginSetting("junit-test", new HashMap<>());
        final Class<Source> clazz = PluginRepository.getSourceClass(testPluginSettings.getName());
        assertNotNull(clazz);
        try {
            PluginFactory.newPlugin(testPluginSettings, clazz);
        } catch (PluginException e) {
            assertTrue("Incorrect exception or exception message was thrown", e.getMessage().startsWith(
                    "Data Prepper plugin requires a constructor with PluginSetting parameter; Plugin " +
                            "ConstructorLessComponent with name junit-test is missing such constructor."));
        }
    }

}
