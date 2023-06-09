/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.oauth;

import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallback;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
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
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class OAuthAuthenticateValidatorCallbackHandlerTest {

    private String saslMechanism;
    private Map<String, String> map = new HashMap<>();
    private List<AppConfigurationEntry> jaasConfigEntries = new ArrayList<>();

    private AppConfigurationEntry appConfigurationEntry;
    @Mock
    private OAuthAuthenticateValidatorCallbackHandler validatorCallbackHandler;

    @BeforeEach
    void setUp() throws Exception {
        when(validatorCallbackHandler.isConfigured()).thenReturn((true));
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
        OAuthAuthenticateValidatorCallbackHandler loginCallbackHandler = new OAuthAuthenticateValidatorCallbackHandler();
        OAuthAuthenticateValidatorCallbackHandler spyHandler = spy(loginCallbackHandler);
        spyHandler.configure(null, saslMechanism, jaasConfigEntries);
    }

    @Test
    @DisplayName("Test and verify invalid SASL mechanism")
    void testConfigurationWithInvalidSaslMechanism() {
        OAuthAuthenticateValidatorCallbackHandler loginCallbackHandler = new OAuthAuthenticateValidatorCallbackHandler();
        OAuthAuthenticateValidatorCallbackHandler spyHandler = spy(loginCallbackHandler);
        Throwable exception = assertThrows(IllegalArgumentException.class,
                () -> spyHandler.configure(null, "sasl", jaasConfigEntries));
        assertEquals("Invalid SASL mechanism: sasl", exception.getMessage());
    }

    @Test
    @DisplayName("Test and verify null JAAS config")
    void testConfigurationWithNullJaas() {
        OAuthAuthenticateValidatorCallbackHandler loginCallbackHandler = new OAuthAuthenticateValidatorCallbackHandler();
        OAuthAuthenticateValidatorCallbackHandler spyHandler = spy(loginCallbackHandler);
        List<AppConfigurationEntry> jaasConfigEntry = new ArrayList<>();
        Throwable exception = assertThrows(IllegalArgumentException.class,
                () -> spyHandler.configure(null, saslMechanism, jaasConfigEntry));
        assertEquals("Must supply exactly one non-null JAAS mechanism configuration (size is : 0)", exception.getMessage());
    }

    @Test
    @DisplayName("Test handle() with configured value is false")
    void testHandleWithConfiguredFalse() {
        OAuthAuthenticateValidatorCallbackHandler loginCallbackHandler = new OAuthAuthenticateValidatorCallbackHandler();
        OAuthAuthenticateValidatorCallbackHandler spyHandler = spy(loginCallbackHandler);
        Throwable exception = assertThrows(IllegalStateException.class,
                () -> spyHandler.handle(new Callback[]{}));
        assertEquals("Callback handler not configured properly...", exception.getMessage());
    }

    @Test
    @DisplayName("Test handle() with Callback exception values")
    void testHandleWithCallbackException() throws NoSuchFieldException, IllegalAccessException {
        String accessToken = "eyJraWQiOiI0VmVuZ3JSaVFoT0pzSUFYZkNMYUZSMHBqOEtDYnlSUmxqMVFfVmZSc2s0IiwiYWxnIjoiUlMyNTYifQ.eyJ2ZXIiOjEsImp0aSI6IkFULmc0cmQ0b0cxdjJtSE0xMy1fM3pDWGktUXRCdm1aS1AtX05iR1o5NUZFcjAiLCJpc3MiOiJodHRwczovL2Rldi03NTU1Mjc5Ni5va3RhLmNvbS9vYXV0aDIvZGVmYXVsdCIsImF1ZCI6ImFwaTovL2RlZmF1bHQiLCJpYXQiOjE2ODYyMzYzNTUsImV4cCI6MTY4NjIzOTk1NSwiY2lkIjoiMG9hOWRrMTdqelVtczZCUzg1ZDciLCJzY3AiOlsia2Fma2EiXSwic3ViIjoiMG9hOWRrMTdqelVtczZCUzg1ZDcifQ.q1KUx-5PStqbCGJ_tn7RKEp02ln-tfjBo5s7G6C4d5vGyVtPVAXcDOCdonjp00BEZ3Yd0Ip7prl-j3r_683pTRGE9qe-O0ii5ntQTedUduYTxpCJ9tkISU0tOTehBwVQwjUwqhrnEGEl5_ZHmNrieAYMAst8lpCUGlqJJtbFRi-0UQAIXHSTW8i44y7-89u8_toNdKHZJcemuh5H9OFl0F6HX0-fNlpYJlf_2hfmXHzssPXXx2iOHf2lO6mwGi_Q2xwMlzTX37WAllCbJpuNdIgyfNtvqSg9V2GhZ7VfxWvpHt9c3026wA2t85BhUtfiFhS7ONwLoNoy0R6t5zI3iA";
        OAuthAuthenticateValidatorCallbackHandler validatorHandler = new OAuthAuthenticateValidatorCallbackHandler();
        OAuthBearerValidatorCallback callback = new OAuthBearerValidatorCallback(accessToken);
        ReflectivelySetField.setField(OAuthAuthenticateValidatorCallbackHandler.class, validatorHandler, "configured", true);
        Throwable exception = assertThrows(NullPointerException.class,
                () -> validatorHandler.handle(new Callback[]{callback}));
        assertEquals(null, exception.getMessage());
    }

    @Test
    @DisplayName("Test close method")
    void testClose() {
        OAuthAuthenticateValidatorCallbackHandler validatorHandler = new OAuthAuthenticateValidatorCallbackHandler();
        OAuthAuthenticateValidatorCallbackHandler spyHandler = spy(validatorHandler);
        spyHandler.close();
    }

    @Test
    @DisplayName("Test handle() with NameCallback exception values")
    void testHandleWithNameCallbackException() throws NoSuchFieldException, IllegalAccessException {
        String accessToken = "eyJraWQiOiI0VmVuZ3JSaVFoT0pzSUFYZkNMYUZSMHBqOEtDYnlSUmxqMVFfVmZSc2s0IiwiYWxnIjoiUlMyNTYifQ.eyJ2ZXIiOjEsImp0aSI6IkFULmc0cmQ0b0cxdjJtSE0xMy1fM3pDWGktUXRCdm1aS1AtX05iR1o5NUZFcjAiLCJpc3MiOiJodHRwczovL2Rldi03NTU1Mjc5Ni5va3RhLmNvbS9vYXV0aDIvZGVmYXVsdCIsImF1ZCI6ImFwaTovL2RlZmF1bHQiLCJpYXQiOjE2ODYyMzYzNTUsImV4cCI6MTY4NjIzOTk1NSwiY2lkIjoiMG9hOWRrMTdqelVtczZCUzg1ZDciLCJzY3AiOlsia2Fma2EiXSwic3ViIjoiMG9hOWRrMTdqelVtczZCUzg1ZDcifQ.q1KUx-5PStqbCGJ_tn7RKEp02ln-tfjBo5s7G6C4d5vGyVtPVAXcDOCdonjp00BEZ3Yd0Ip7prl-j3r_683pTRGE9qe-O0ii5ntQTedUduYTxpCJ9tkISU0tOTehBwVQwjUwqhrnEGEl5_ZHmNrieAYMAst8lpCUGlqJJtbFRi-0UQAIXHSTW8i44y7-89u8_toNdKHZJcemuh5H9OFl0F6HX0-fNlpYJlf_2hfmXHzssPXXx2iOHf2lO6mwGi_Q2xwMlzTX37WAllCbJpuNdIgyfNtvqSg9V2GhZ7VfxWvpHt9c3026wA2t85BhUtfiFhS7ONwLoNoy0R6t5zI3iA";
        OAuthAuthenticateValidatorCallbackHandler validatorHandler = new OAuthAuthenticateValidatorCallbackHandler();
        NameCallback callback = new NameCallback("callback");
        ReflectivelySetField.setField(OAuthAuthenticateValidatorCallbackHandler.class, validatorHandler, "configured", true);
        Throwable exception = assertThrows(UnsupportedCallbackException.class,
                () -> validatorHandler.handle(new Callback[]{callback}));
        assertEquals(null, exception.getMessage());
    }

}
