package org.opensearch.dataprepper.plugins.source.jira.rest;

import org.opensearch.dataprepper.plugins.source.jira.rest.auth.JiraAuthConfig;
import org.opensearch.dataprepper.plugins.source.jira.rest.auth.JiraOauthConfig;
import org.springframework.http.HttpRequest;
import org.springframework.http.client.ClientHttpRequestExecution;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.client.ClientHttpResponse;

import java.io.IOException;

public class OAuth2RequestInterceptor implements ClientHttpRequestInterceptor {

    private final JiraAuthConfig config;

    public OAuth2RequestInterceptor(JiraAuthConfig config) {
        this.config = config;
    }

    @Override
    public ClientHttpResponse intercept(HttpRequest request, byte[] body, ClientHttpRequestExecution execution) throws IOException {
        request.getHeaders().setBearerAuth(((JiraOauthConfig) config).getAccessToken());
        return execution.execute(request, body);
    }

}
