package org.opensearch.dataprepper.plugins.source.saas.jira.rest;

import org.opensearch.dataprepper.plugins.source.saas.jira.JiraSourceConfig;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpRequest;
import org.springframework.http.client.ClientHttpRequestExecution;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.client.ClientHttpResponse;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;


public class BasicAuthInterceptor implements ClientHttpRequestInterceptor {
    private final String username;
    private final String password;

    public BasicAuthInterceptor(JiraSourceConfig config) {
        this.username = config.getJiraId();
        this.password = config.getJiraCredential();
    }

    @Override
    public ClientHttpResponse intercept(HttpRequest request, byte[] body, ClientHttpRequestExecution execution) throws IOException {
        String auth = username + ":" + password;
        byte[] encodedAuth = Base64.getEncoder().encode(auth.getBytes(StandardCharsets.US_ASCII));
        String authHeader = "Basic " + new String(encodedAuth);
        request.getHeaders().set(HttpHeaders.AUTHORIZATION, authHeader);
        return execution.execute(request, body);
    }
}
