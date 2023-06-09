/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.oauth;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;


import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class OAuthHttpCallsTest {

    private String saslMechanism;
    private String accessToken;
    private Map<String, String> map = new HashMap<>();
@Mock
    OAuthHttpCalls httpCall;
    @BeforeEach
    void setUp() throws Exception {
        //when(httpCall.sslConfigured()).thenReturn((false));
        saslMechanism = "OAUTHBEARER";
        map.put("OAUTH_INTROSPECT_SERVER", "dev-75552796.okta.com");
        map.put("OAUTH_INTROSPECT_AUTHORIZATION", "Basic MG9hOWRrMTdqelVtczZCUzg1ZDc6Ql9GNGF6VFpoVFNpeWlRdUR1cC1sb24tQU9kdnFUNmNNQTVDdm5vaw==");
        map.put("OAUTH_LOGIN_GRANT_TYPE", "client_credentials");
        map.put("OAUTH_LOGIN_SERVER", "dev-75552796.okta.com");
        map.put("OAUTH_LOGIN_SCOPE", "kafka");
        map.put("OAUTH_LOGIN_ENDPOINT", "/oauth2/default/v1/token");
        map.put("OAUTH_AUTHORIZATION", "Basic MG9hOWRrMTdqelVtczZCUzg1ZDc6Ql9GNGF6VFpoVFNpeWlRdUR1cC1sb24tQU9kdnFUNmNNQTVDdm5vaw==");
        map.put("OAUTH_INTROSPECT_ENDPOINT", "/oauth2/default/v1/introspect");
        accessToken = "eyJraWQiOiI0VmVuZ3JSaVFoT0pzSUFYZkNMYUZSMHBqOEtDYnlSUmxqMVFfVmZSc2s0IiwiYWxnIjoiUlMyNTYifQ.eyJ2ZXIiOjEsImp0aSI6IkFULmc0cmQ0b0cxdjJtSE0xMy1fM3pDWGktUXRCdm1aS1AtX05iR1o5NUZFcjAiLCJpc3MiOiJodHRwczovL2Rldi03NTU1Mjc5Ni5va3RhLmNvbS9vYXV0aDIvZGVmYXVsdCIsImF1ZCI6ImFwaTovL2RlZmF1bHQiLCJpYXQiOjE2ODYyMzYzNTUsImV4cCI6MTY4NjIzOTk1NSwiY2lkIjoiMG9hOWRrMTdqelVtczZCUzg1ZDciLCJzY3AiOlsia2Fma2EiXSwic3ViIjoiMG9hOWRrMTdqelVtczZCUzg1ZDcifQ.q1KUx-5PStqbCGJ_tn7RKEp02ln-tfjBo5s7G6C4d5vGyVtPVAXcDOCdonjp00BEZ3Yd0Ip7prl-j3r_683pTRGE9qe-O0ii5ntQTedUduYTxpCJ9tkISU0tOTehBwVQwjUwqhrnEGEl5_ZHmNrieAYMAst8lpCUGlqJJtbFRi-0UQAIXHSTW8i44y7-89u8_toNdKHZJcemuh5H9OFl0F6HX0-fNlpYJlf_2hfmXHzssPXXx2iOHf2lO6mwGi_Q2xwMlzTX37WAllCbJpuNdIgyfNtvqSg9V2GhZ7VfxWvpHt9c3026wA2t85BhUtfiFhS7ONwLoNoy0R6t5zI3iA";
    }

    @Test
    @DisplayName("Test OAuthBearerTokenJwt Login()")
    void testLogin() {
        assertNotNull(OAuthHttpCalls.login(map));
    }

    @Test
    @DisplayName("Test IntrospectBearer Token")
    void testintrospectBearer() {
        OAuthHttpCalls.introspectBearer(map, accessToken);
    }

    @Test
    @DisplayName("Test accept UnsecureServer")
    void testacceptUnsecureServer() throws NoSuchFieldException, IllegalAccessException {
        OAuthHttpCalls oAuthHttpCalls = new OAuthHttpCalls();
        ReflectivelySetField.setField(OAuthHttpCalls.class, oAuthHttpCalls, "OAUTH_ACCEPT_UNSECURE_SERVER", true);
        OAuthHttpCalls.acceptUnsecureServer();
    }

}
