/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.model.plugin.kafka;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * * A helper class that helps to read oauth configuration values from the
 * pipelines.yaml
 */
public class OAuthConfig {
    private static final String OAUTH_LOGIN_GRANT_TYPE = "refresh_token";
    private static final String OAUTH_LOGIN_SCOPE = "kafka";
    private static final String OAUTH_SASL_MECHANISM = "OAUTHBEARER";
    private static final String OAUTH_SECURITY_PROTOCOL = "SASL_PLAINTEXT";
    private static final String OAUTH_SASL_LOGIN_CALLBACK_HANDLER_CLASS = "org.apache.kafka.common.security.oauthbearer.secured.OAuthBearerLoginCallbackHandler";
    private static final String OAUTH_INTROSPECT_ENDPOINT = "/oauth2/default/v1/introspect";
    @JsonProperty("oauth_client_id")
    private String oauthClientId;
    @JsonProperty("oauth_client_secret")
    private String oauthClientSecret;
    @JsonProperty("oauth_login_server")
    private String oauthLoginServer;
    @JsonProperty("oauth_login_endpoint")
    private String oauthLoginEndpoint;
    @JsonProperty("oauth_login_grant_type")
    private String oauthLoginGrantType = OAUTH_LOGIN_GRANT_TYPE;
    @JsonProperty("oauth_login_scope")
    private String oauthLoginScope = OAUTH_LOGIN_SCOPE;
    @JsonProperty("oauth_authorization_token")
    private String oauthAuthorizationToken;
    @JsonProperty("oauth_introspect_server")
    private String oauthIntrospectServer = "";
    @JsonProperty("oauth_introspect_endpoint")
    private String oauthIntrospectEndpoint = "";
    @JsonProperty("oauth_introspect_authorization_token")
    private String oauthIntrospectAuthorizationToken;
    @JsonProperty("oauth_token_endpoint_url")
    private String oauthTokenEndpointURL;
    @JsonProperty("oauth_sasl_mechanism")
    private String oauthSaslMechanism = OAUTH_SASL_MECHANISM;
    @JsonProperty("oauth_security_protocol")
    private String oauthSecurityProtocol = OAUTH_SECURITY_PROTOCOL;
    @JsonProperty("oauth_sasl_login_callback_handler_class")
    private String oauthSaslLoginCallbackHandlerClass = OAUTH_SASL_LOGIN_CALLBACK_HANDLER_CLASS;

    @JsonProperty("oauth_jwks_endpoint_url")
    private String oauthJwksEndpointURL = "";

    @JsonProperty("extension_logicalCluster")
    private String extensionLogicalCluster;

    @JsonProperty("extension_identityPoolId")
    private String extensionIdentityPoolId;

    public String getOauthAuthorizationToken() {
        return oauthAuthorizationToken;
    }

    public String getOauthIntrospectAuthorizationToken() {
        return oauthIntrospectAuthorizationToken;
    }

    public String getExtensionLogicalCluster() {
        return extensionLogicalCluster;
    }

    public String getExtensionIdentityPoolId() {
        return extensionIdentityPoolId;
    }

    public String getOauthJwksEndpointURL() {
        return oauthJwksEndpointURL;
    }

    public String getOauthSaslMechanism() {
        return oauthSaslMechanism;
    }

    public String getOauthSecurityProtocol() {
        return oauthSecurityProtocol;
    }

    public String getOauthSaslLoginCallbackHandlerClass() {
        return oauthSaslLoginCallbackHandlerClass;
    }

    public String getOauthClientId() {
        return oauthClientId;
    }

    public String getOauthClientSecret() {
        return oauthClientSecret;
    }

    public String getOauthTokenEndpointURL() {
        return oauthTokenEndpointURL;
    }

    public String getOauthLoginServer() {
        return oauthLoginServer;
    }

    public void setOauthLoginServer(String oauthLoginServer) {
        this.oauthLoginServer = oauthLoginServer;
    }

    public String getOauthLoginEndpoint() {
        return oauthLoginEndpoint;
    }

    public void setOauthLoginEndpoint(String oauthLoginEndpoint) {
        this.oauthLoginEndpoint = oauthLoginEndpoint;
    }

    public String getOauthLoginGrantType() {
        return oauthLoginGrantType;
    }

    public void setOauthLoginGrantType(String oauthLoginGrantType) {
        this.oauthLoginGrantType = oauthLoginGrantType;
    }

    public String getOauthLoginScope() {
        return oauthLoginScope;
    }

    public void setOauthLoginScope(String oauthLoginScope) {
        this.oauthLoginScope = oauthLoginScope;
    }

    public String getOauthIntrospectServer() {
        /*if (oauthIntrospectServer.isEmpty() || oauthIntrospectServer.isBlank()) {
            return oauthLoginServer;
        }*/
        return oauthIntrospectServer;
    }

    public void setOauthIntrospectServer(String oauthIntrospectServer) {
        this.oauthIntrospectServer = oauthIntrospectServer;
    }

    public String getOauthIntrospectEndpoint() {
        if (!oauthIntrospectServer.isEmpty() || !oauthIntrospectServer.isBlank()) {
            return OAUTH_INTROSPECT_ENDPOINT;
        }
        return oauthIntrospectEndpoint;
    }

    public void setOauthIntrospectEndpoint(String oauthIntrospectEndpoint) {
        this.oauthIntrospectEndpoint = oauthIntrospectEndpoint;
    }
}
