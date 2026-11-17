/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.gcp;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.gcp.api.GcpCredentialsSupplier;
import org.opensearch.dataprepper.model.annotations.DataPrepperExtensionPlugin;
import org.opensearch.dataprepper.model.annotations.ExtensionProvides;
import org.opensearch.dataprepper.model.plugin.ExtensionPoints;
import org.opensearch.dataprepper.model.plugin.ExtensionProvider;

import java.util.Optional;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class GcpPluginIT {
    @Mock
    private ExtensionPoints extensionPoints;

    @Mock
    private ExtensionProvider.Context context;

    private GcpCredentialsSupplier applyAndGetSupplier(final GcpPluginConfig config) {
        new GcpPlugin(config).apply(extensionPoints);
        final ArgumentCaptor<ExtensionProvider> captor = ArgumentCaptor.forClass(ExtensionProvider.class);
        verify(extensionPoints).addExtensionProvider(captor.capture());
        assertThat(captor.getValue(), instanceOf(GcpExtensionProvider.class));
        return (GcpCredentialsSupplier) captor.getValue().provideInstance(context).orElseThrow();
    }

    @Test
    void annotations_use_configurations_root_key_and_provide_supplier() {
        final DataPrepperExtensionPlugin plugin = GcpPlugin.class.getAnnotation(DataPrepperExtensionPlugin.class);
        final ExtensionProvides provides = GcpPlugin.class.getAnnotation(ExtensionProvides.class);

        assertThat(plugin.rootKeyJsonPath(), equalTo("/gcp/configurations"));
        assertThat(plugin.modelType(), equalTo(GcpPluginConfig.class));
        assertThat(provides.providedClasses()[0], equalTo(GcpCredentialsSupplier.class));
    }

    @Test
    void apply_with_default_project_registers_supplier_returning_it() throws Exception {
        final GcpPluginConfig config = new ObjectMapper(new YAMLFactory())
                .readValue("default:\n  project_id: configured-project\n  auth_mode: adc\n", GcpPluginConfig.class);

        assertThat(applyAndGetSupplier(config).getDefaultProjectId(), equalTo(Optional.of("configured-project")));
    }

    @Test
    void apply_without_config_registers_supplier() {
        assertThat(applyAndGetSupplier(null), instanceOf(DefaultGcpCredentialsSupplier.class));
    }

    @Test
    void apply_with_config_without_default_registers_supplier() throws Exception {
        final GcpPluginConfig config = new ObjectMapper(new YAMLFactory()).readValue("{}", GcpPluginConfig.class);

        assertThat(applyAndGetSupplier(config), instanceOf(DefaultGcpCredentialsSupplier.class));
    }
}
