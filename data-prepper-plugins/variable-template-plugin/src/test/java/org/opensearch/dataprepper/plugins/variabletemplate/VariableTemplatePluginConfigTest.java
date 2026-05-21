/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.variabletemplate;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

class VariableTemplatePluginConfigTest {

    @Test
    void testDefaults_envAndFileDisabled() {
        final VariableTemplatePluginConfig config = new VariableTemplatePluginConfig();
        assertThat(config.getResolvers(), notNullValue());
        assertThat(config.getResolvers().isEnvEnabled(), is(false));
        assertThat(config.getResolvers().isFileEnabled(), is(false));
        assertThat(config.getResolvers().getStore(), equalTo(null));
    }

    @Test
    void testStoreResolverConfig_defaults() {
        final VariableTemplatePluginConfig.StoreResolverConfig storeConfig =
                new VariableTemplatePluginConfig.StoreResolverConfig();
        assertThat(storeConfig.isEnabled(), is(false));
        assertThat(storeConfig.getSources(), equalTo(List.of()));
    }
}
