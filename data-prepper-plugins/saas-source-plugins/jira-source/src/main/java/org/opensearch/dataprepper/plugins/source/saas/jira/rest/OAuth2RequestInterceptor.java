package org.opensearch.dataprepper.plugins.source.saas.jira.rest;

import org.opensearch.dataprepper.plugins.source.saas.jira.models.JiraOauthConfig;
import org.springframework.http.HttpRequest;
import org.springframework.http.client.ClientHttpRequestExecution;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.client.ClientHttpResponse;

import java.io.IOException;

public class OAuth2RequestInterceptor implements ClientHttpRequestInterceptor {

    private final JiraOauthConfig config;

    public OAuth2RequestInterceptor(JiraOauthConfig config) {
        this.config = config;
    }

    @Override
    public ClientHttpResponse intercept(HttpRequest request, byte[] body, ClientHttpRequestExecution execution) throws IOException {
        request.getHeaders().setBearerAuth(config.getAccessToken());
        return execution.execute(request, body);
    }

}
