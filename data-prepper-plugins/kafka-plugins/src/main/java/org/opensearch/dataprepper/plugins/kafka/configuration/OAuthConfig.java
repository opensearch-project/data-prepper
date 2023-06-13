package org.opensearch.dataprepper.plugins.kafka.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

public class OAuthConfig {
    @JsonProperty("oauth_client_id")
    private String oauthClientId;
    @JsonProperty("oauth_client_secret")
    private String oauthClientSecret;
    @JsonProperty("oauth_login_server")
    private String oauthLoginServer;
    @JsonProperty("oauth_login_endpoint")
    private String oauthLoginEndpoint;
    @JsonProperty("oauth_login_grant_type")
    private String oauthLoginGrantType;
    @JsonProperty("oauth_login_scope")
    private String oauthLoginScope;
    @JsonProperty("oauth_authorization_token")
    private String oauthAuthorizationToken;
    @JsonProperty("oauth_introspect_server")
    private String oauthIntrospectServer;
    @JsonProperty("oauth_introspect_endpoint")
    private String oauthIntrospectEndpoint;
    @JsonProperty("oauth_introspect_authorization_token")
    private String oauthIntrospectAuthorizationToken;
    @JsonProperty("oauth_token_endpoint_url")
    private String oauthTokenEndpointURL;
    @JsonProperty("oauth_sasl_mechanism")
    private String oauthSaslMechanism;
    @JsonProperty("oauth_security_protocol")
    private String oauthSecurityProtocol;
    @JsonProperty("oauth_sasl_login_callback_handler_class")
    private String oauthSaslLoginCallbackHandlerClass;

    @JsonProperty("oauth_jwks_endpoint_url")
    private String oauthJwksEndpointURL;

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
        return oauthIntrospectServer;
    }

    public void setOauthIntrospectServer(String oauthIntrospectServer) {
        this.oauthIntrospectServer = oauthIntrospectServer;
    }

    public String getOauthIntrospectEndpoint() {
        return oauthIntrospectEndpoint;
    }

    public void setOauthIntrospectEndpoint(String oauthIntrospectEndpoint) {
        this.oauthIntrospectEndpoint = oauthIntrospectEndpoint;
    }

    public String getOauthAuthorizationToken() {
        return oauthAuthorizationToken;
    }

    public void setOauthAuthorizationToken(String oauthAuthorizationToken) {
        this.oauthAuthorizationToken = oauthAuthorizationToken;
    }

    public String getOauthIntrospectAuthorizationToken() {
        return oauthIntrospectAuthorizationToken;
    }

    public void setOauthIntrospectAuthorizationToken(String oauthIntrospectAuthorizationToken) {
        this.oauthIntrospectAuthorizationToken = oauthIntrospectAuthorizationToken;
    }
}
