package org.opensearch.dataprepper.plugins.kafka.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

public class OAuthConfig {
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

    @JsonProperty("oauth_introspect_endpoint")
    private String oauthIntrospectEndpoint;

    public String getOauthLoginServer() {
        return oauthLoginServer;
    }

    public String getOauthLoginEndpoint() {
        return oauthLoginEndpoint;
    }

    public String getOauthLoginGrantType() {
        return oauthLoginGrantType;
    }

    public String getOauthLoginScope() {
        return oauthLoginScope;
    }

    public String getOauthIntrospectEndpoint() {
        return oauthIntrospectEndpoint;
    }

    public String getOauthAuthorizationToken() {
        return oauthAuthorizationToken;
    }
}
