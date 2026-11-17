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
import com.google.auth.oauth2.GoogleCredentials;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.gcp.api.GcpCredentialsOptions;
import org.opensearch.dataprepper.gcp.api.GcpCredentialsSupplier;
import org.opensearch.dataprepper.model.annotations.DataPrepperExtensionPlugin;
import org.opensearch.dataprepper.model.annotations.ExtensionProvides;
import org.opensearch.dataprepper.model.plugin.ExtensionPoints;
import org.opensearch.dataprepper.model.plugin.ExtensionProvider;

import java.io.IOException;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class GcpPluginTest {
    private static final String CLIENT_EMAIL = "sa@my-project.iam.gserviceaccount.com";

    @Mock
    private ExtensionPoints extensionPoints;

    @Mock
    private ExtensionProvider.Context context;

    @Mock
    private GoogleCredentials applicationDefaultCredentials;

    @Mock
    private GoogleCredentials scopedCredentials;

    private static GcpPluginConfig readConfig(final String yaml) throws IOException {
        return new ObjectMapper(new YAMLFactory()).readValue(yaml, GcpPluginConfig.class);
    }

    private static String generatePkcs8PrivateKey() throws NoSuchAlgorithmException {
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        final String body = Base64.getMimeEncoder(64, "\n".getBytes())
                .encodeToString(generator.generateKeyPair().getPrivate().getEncoded());
        return "-----BEGIN PRIVATE KEY-----\n" + body + "\n-----END PRIVATE KEY-----\n";
    }

    private static String serviceAccountDefault(final String privateKey, final String tokenUri) {
        final String indentedKey = privateKey.replace("\n", "\\n");
        return "default:\n  auth_mode: service_account_key\n  client_email: " + CLIENT_EMAIL + "\n"
                + "  private_key: \"" + indentedKey + "\"\n  token_uri: " + tokenUri + "\n";
    }

    private GcpCredentialsSupplier applyAndGetSupplier(final GcpPlugin objectUnderTest) {
        objectUnderTest.apply(extensionPoints);
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
        final GcpPluginConfig config = readConfig("default:\n  project_id: configured-project\n  auth_mode: adc\n");

        assertThat(applyAndGetSupplier(new GcpPlugin(config)).getDefaultProjectId(), equalTo(Optional.of("configured-project")));
    }

    @Test
    void getCredentials_without_config_uses_application_default_credentials() {
        when(applicationDefaultCredentials.createScoped(List.of(GcpCredentialsOptions.DEFAULT_SCOPE))).thenReturn(scopedCredentials);

        final GcpCredentialsSupplier supplier = applyAndGetSupplier(new GcpPlugin(null, () -> applicationDefaultCredentials));

        assertThat(supplier.getCredentials(null), sameInstance(scopedCredentials));
    }

    @Test
    void getCredentials_with_config_without_default_uses_application_default_credentials() throws Exception {
        when(applicationDefaultCredentials.createScoped(List.of("scope-a"))).thenReturn(scopedCredentials);

        final GcpCredentialsSupplier supplier = applyAndGetSupplier(new GcpPlugin(readConfig("{}"), () -> applicationDefaultCredentials));

        assertThat(supplier.getCredentials(GcpCredentialsOptions.builder().withScopes(List.of("scope-a")).build()),
                sameInstance(scopedCredentials));
    }

    @Test
    void constructor_with_service_account_key_default_builds_its_credentials_at_startup() throws Exception {
        final GcpPluginConfig config = readConfig(serviceAccountDefault(generatePkcs8PrivateKey(), GcpCredentialsOptions.DEFAULT_TOKEN_URI));

        final GcpCredentialsSupplier supplier = applyAndGetSupplier(new GcpPlugin(config, () -> applicationDefaultCredentials));

        assertThat(supplier.getCredentials(null), sameInstance(supplier.getCredentials(config.getDefaultConfig().toCredentialsOptions())));
    }

    @Test
    void constructor_with_disallowed_default_token_uri_fails_at_startup() throws Exception {
        final GcpPluginConfig config = readConfig(serviceAccountDefault(generatePkcs8PrivateKey(), "https://evil.example.com/token"));

        final IllegalArgumentException exception =
                assertThrows(IllegalArgumentException.class, () -> new GcpPlugin(config, () -> applicationDefaultCredentials));

        assertThat(exception.getMessage(), containsString("token_uri host must be oauth2.googleapis.com"));
    }
}
