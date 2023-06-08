/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.oauth;

import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class OAuthBearerTokenJwt implements OAuthBearerToken {

    private String value;
    private long lifetimeMs;
    private String principalName;
    private Long startTimeMs;
    private Set<String> scope;
    private long expirationTime;
    private String jti;
    private static final String SCOPE = "scope";
    private static final String EXP = "exp";
    private static final String IAT = "iat";

    public OAuthBearerTokenJwt(String accessToken, long lifeTimeS, long startTimeMs, String principalName) {
        super();
        this.value = accessToken;
        this.principalName = principalName;
        this.lifetimeMs = startTimeMs + (lifeTimeS * 1000);
        this.startTimeMs = startTimeMs;
        this.expirationTime = startTimeMs + (lifeTimeS * 1000);
    }

    public OAuthBearerTokenJwt(Map<String, Object> jwtToken, String accessToken) {
        super();
        this.value = accessToken;
        this.principalName = (String) jwtToken.get("sub");

        if (this.scope == null) {
            this.scope = new TreeSet<>();
        }
        if (jwtToken.get(SCOPE) instanceof String) {
            this.scope.add((String) jwtToken.get(SCOPE));
        } else if (jwtToken.get(SCOPE) instanceof List) {
            for (String s : (List<String>) jwtToken.get(SCOPE)) {
                this.scope.add(s);
            }
        }

        Object exp = jwtToken.get(EXP);
        if (exp instanceof Integer) {
            this.expirationTime = Integer.toUnsignedLong((Integer) jwtToken.get(EXP));
        } else {
            this.expirationTime = (Long) jwtToken.get(EXP);
        }

        Object iat = jwtToken.get(IAT);
        if (iat instanceof Integer) {
            this.startTimeMs = Integer.toUnsignedLong((Integer) jwtToken.get(IAT));
        } else {
            this.startTimeMs = (Long) jwtToken.get(IAT);
        }

        this.lifetimeMs = expirationTime;
        this.jti = (String) jwtToken.get("jti");
    }

    @Override
    public String value() {
        return value;
    }

    @Override
    public Set<String> scope() {
        return scope;
    }

    @Override
    public long lifetimeMs() {
        return lifetimeMs;
    }

    @Override
    public String principalName() {
        return principalName;
    }

    @Override
    public Long startTimeMs() {
        return startTimeMs != null ? startTimeMs : 0;
    }

    public long expirationTime() {
        return expirationTime;
    }

    public String jti() {
        return jti;
    }

    @Override
    public String toString() {
        return "OAuthBearerTokenJwt{" +
                "value='" + value + '\'' +
                ", lifetimeMs=" + lifetimeMs +
                ", principalName='" + principalName + '\'' +
                ", startTimeMs=" + startTimeMs +
                ", scope=" + scope +
                ", expirationTime=" + expirationTime +
                ", jti='" + jti + '\'' +
                '}';
    }
}