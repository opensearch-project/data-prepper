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

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class OAuthBearerTokenJwtTest {

    @Mock
    private OAuthBearerTokenJwt oAuthBearerTokenJwt;

    private String accessToken;
    private long lifetimeMs;
    private String principalName;
    private Long startTimeMs;
    private Set<String> scope;
    private long expirationTime;

    Map<String, Object> jwtToken;
    @BeforeEach
    void setUp() throws Exception {
        jwtToken = new HashMap<>();
        jwtToken.put("sub","Test");
        jwtToken.put("scope","public");
        jwtToken.put("exp",123456L);
        jwtToken.put("iat",123456L);
        jwtToken.put("jti","ABCDE");
        accessToken = "eyJraWQiOiI0VmVuZ3JSaVFoT0pzSUFYZkNMYUZSMHBqOEtDYnlSUmxqMVFfVmZSc2s0IiwiYWxnIjoiUlMyNTYifQ.eyJ2ZXIiOjEsImp0aSI6IkFULmc0cmQ0b0cxdjJtSE0xMy1fM3pDWGktUXRCdm1aS1AtX05iR1o5NUZFcjAiLCJpc3MiOiJodHRwczovL2Rldi03NTU1Mjc5Ni5va3RhLmNvbS9vYXV0aDIvZGVmYXVsdCIsImF1ZCI6ImFwaTovL2RlZmF1bHQiLCJpYXQiOjE2ODYyMzYzNTUsImV4cCI6MTY4NjIzOTk1NSwiY2lkIjoiMG9hOWRrMTdqelVtczZCUzg1ZDciLCJzY3AiOlsia2Fma2EiXSwic3ViIjoiMG9hOWRrMTdqelVtczZCUzg1ZDcifQ.q1KUx-5PStqbCGJ_tn7RKEp02ln-tfjBo5s7G6C4d5vGyVtPVAXcDOCdonjp00BEZ3Yd0Ip7prl-j3r_683pTRGE9qe-O0ii5ntQTedUduYTxpCJ9tkISU0tOTehBwVQwjUwqhrnEGEl5_ZHmNrieAYMAst8lpCUGlqJJtbFRi-0UQAIXHSTW8i44y7-89u8_toNdKHZJcemuh5H9OFl0F6HX0-fNlpYJlf_2hfmXHzssPXXx2iOHf2lO6mwGi_Q2xwMlzTX37WAllCbJpuNdIgyfNtvqSg9V2GhZ7VfxWvpHt9c3026wA2t85BhUtfiFhS7ONwLoNoy0R6t5zI3iA";
        lifetimeMs = 123456L;
        principalName = "Test";
        startTimeMs = 123456L;
        expirationTime = 123456L;
        scope = new HashSet<>();
        scope.add("public");
    }

    @Test
    @DisplayName("Test OAuthBearerTokenJwt() method")
    void testOAuthBearerTokenJwt() {
        OAuthBearerTokenJwt tokenJwt = new OAuthBearerTokenJwt(accessToken, lifetimeMs, startTimeMs, principalName);
        OAuthBearerTokenJwt tokenJWT = new OAuthBearerTokenJwt(jwtToken, accessToken);
        OAuthBearerTokenJwt spyJwt = spy(tokenJwt);
    }

    @Test
    @DisplayName("Test OAuthBearerTokenJwt() method with Integer instance")
    void testOAuthBearerTokenJwtWithIntInstance() {
        Map<String, Object> token = new HashMap<>();
        List<String> list = new ArrayList();
        list.add("private");
        list.add("public");
        token.put("sub","Test");
        token.put("scope",list);
        token.put("exp",12345);
        token.put("iat",12345);
        token.put("jti","ABCDE");
        OAuthBearerTokenJwt tokenJwt = new OAuthBearerTokenJwt(accessToken, lifetimeMs, startTimeMs, principalName);
        OAuthBearerTokenJwt tokenJWT = new OAuthBearerTokenJwt(token, accessToken);
        OAuthBearerTokenJwt spyJwt = spy(tokenJwt);
    }

    @Test
    @DisplayName("Test all methods")
    void testAllMethods() {
        OAuthBearerTokenJwt tokenJWT = new OAuthBearerTokenJwt(jwtToken, accessToken);
        OAuthBearerTokenJwt spyJwt = spy(tokenJWT);
        assertNotNull(spyJwt.jti());
        assertNotNull(spyJwt.expirationTime());
        assertNotNull(spyJwt.value());
        assertNotNull(spyJwt.scope());
        assertNotNull(spyJwt.principalName());
        assertNotNull(spyJwt.lifetimeMs());
        assertNotNull(spyJwt.startTimeMs());
        assertNotNull(spyJwt.toString());
    }

}
