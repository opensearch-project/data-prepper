/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.oauth;

import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import org.mockito.Mock;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.spy;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class OAuthAuthenticateLoginCallbackHandlerTest {

    private String saslMechanism;
    private Map<String, String> map = new HashMap<>();
    private List<AppConfigurationEntry> jaasConfigEntries = new ArrayList<>();

    private AppConfigurationEntry appConfigurationEntry;
    @Mock
    private OAuthAuthenticateLoginCallbackHandler loginCallbackHandler;

    @BeforeEach
    void setUp() throws Exception {
        saslMechanism = "OAUTHBEARER";
        map.put("OAUTH_INTROSPECT_SERVER", "dev-75552796.okta.com");
        map.put("OAUTH_INTROSPECT_AUTHORIZATION", "Basic MG9hOWRrMTdqelVtczZCUzg1ZDc6Ql9GNGF6VFpoVFNpeWlRdUR1cC1sb24tQU9kdnFUNmNNQTVDdm5vaw==");
        map.put("OAUTH_LOGIN_GRANT_TYPE", "client_credentials");
        map.put("OAUTH_LOGIN_SERVER", "dev-75552796.okta.com");
        map.put("OAUTH_LOGIN_SCOPE", "kafka");
        map.put("OAUTH_LOGIN_ENDPOINT", "/oauth2/default/v1/token");
        map.put("OAUTH_AUTHORIZATION", "Basic MG9hOWRrMTdqelVtczZCUzg1ZDc6Ql9GNGF6VFpoVFNpeWlRdUR1cC1sb24tQU9kdnFUNmNNQTVDdm5vaw==");
        map.put("OAUTH_INTROSPECT_ENDPOINT", "/oauth2/default/v1/introspect");

        AppConfigurationEntry.LoginModuleControlFlag controlFlag = AppConfigurationEntry.LoginModuleControlFlag.REQUIRED;
        appConfigurationEntry = new AppConfigurationEntry("Test", controlFlag, map);
        jaasConfigEntries.add(appConfigurationEntry);
    }

    @Test
    @DisplayName("Test and verify SASL and JASS mechanism")
    void testConfiguration() {
        OAuthAuthenticateLoginCallbackHandler loginCallbackHandler = new OAuthAuthenticateLoginCallbackHandler();
        OAuthAuthenticateLoginCallbackHandler spyHandler = spy(loginCallbackHandler);
        spyHandler.configure(null, saslMechanism, jaasConfigEntries);
    }

    @Test
    @DisplayName("Test and verify invalid SASL mechanism")
    void testConfigurationWithInvalidSaslMechanism() {
        OAuthAuthenticateLoginCallbackHandler loginCallbackHandler = new OAuthAuthenticateLoginCallbackHandler();
        OAuthAuthenticateLoginCallbackHandler spyHandler = spy(loginCallbackHandler);
        Throwable exception = assertThrows(IllegalArgumentException.class,
                () -> spyHandler.configure(null, "sasl", jaasConfigEntries));
        assertEquals("Invalid SASL mechanism: sasl", exception.getMessage());
    }

    @Test
    @DisplayName("Test and verify null JAAS config")
    void testConfigurationWithNullJaas() {
        OAuthAuthenticateLoginCallbackHandler loginCallbackHandler = new OAuthAuthenticateLoginCallbackHandler();
        OAuthAuthenticateLoginCallbackHandler spyHandler = spy(loginCallbackHandler);
        List<AppConfigurationEntry> jaasConfigEntry = new ArrayList<>();
        Throwable exception = assertThrows(IllegalArgumentException.class,
                () -> spyHandler.configure(null, saslMechanism, jaasConfigEntry));
        assertEquals("Must supply exactly one non-null JAAS mechanism configuration (size is : 0)", exception.getMessage());
    }

    @Test
    @DisplayName("Test handle() with configured value is false")
    void testHandleWithConfiguredFalse() {
        OAuthAuthenticateLoginCallbackHandler loginCallbackHandler = new OAuthAuthenticateLoginCallbackHandler();
        OAuthAuthenticateLoginCallbackHandler spyHandler = spy(loginCallbackHandler);
        Throwable exception = assertThrows(IllegalStateException.class,
                () -> spyHandler.handle(new Callback[]{}));
        assertEquals("Callback handler not configured properly...", exception.getMessage());
    }

    @Test
    @DisplayName("Test handle() with Callback exception values")
    void testHandleWithCallbackException() throws IOException, UnsupportedCallbackException, NoSuchFieldException, IllegalAccessException {
        OAuthAuthenticateLoginCallbackHandler loginCallbackHandler = new OAuthAuthenticateLoginCallbackHandler();
        OAuthBearerTokenCallback callback = new OAuthBearerTokenCallback();
        ReflectivelySetField.setField(OAuthAuthenticateLoginCallbackHandler.class, loginCallbackHandler, "configured", true);
        Throwable exception = assertThrows(IllegalArgumentException.class,
                () -> loginCallbackHandler.handle(new Callback[]{callback}));
        assertEquals("Null or empty token returned from the server...", exception.getMessage());
    }

    @Test
    @DisplayName("Test the close function")
    void testClose() {
        OAuthAuthenticateLoginCallbackHandler loginCallbackHandler = new OAuthAuthenticateLoginCallbackHandler();
        OAuthAuthenticateLoginCallbackHandler spyHandler = spy(loginCallbackHandler);
        spyHandler.close();
    }
}
