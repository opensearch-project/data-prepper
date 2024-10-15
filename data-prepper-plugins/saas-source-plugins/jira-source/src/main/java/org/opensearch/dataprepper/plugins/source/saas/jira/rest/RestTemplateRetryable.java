package org.opensearch.dataprepper.plugins.source.saas.jira.rest;

import org.springframework.http.HttpStatus;
import org.springframework.lang.NonNull;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.net.URI;
import java.util.Map;

public class RestTemplateRetryable extends RestTemplate {

    private final RetryTemplate retryTemplate;

    public RestTemplateRetryable(int retryMaxAttempts) {
        this.retryTemplate = new CustomRetryTemplateBuilder()
                .withRetryMaxAttempts(retryMaxAttempts)
                .withHttpStatus(HttpStatus.TOO_MANY_REQUESTS)
                .withHttpStatus(HttpStatus.BAD_GATEWAY)
                .withHttpStatus(HttpStatus.GATEWAY_TIMEOUT)
                .withHttpStatus(HttpStatus.SERVICE_UNAVAILABLE)
                .build();
    }

    @Override
    public <T> T getForObject(@NonNull URI url, @NonNull Class<T> responseType) throws RestClientException {
        return retryTemplate.execute(retryContext ->
                super.getForObject(url, responseType));
    }

    @Override
    public <T> T getForObject(@NonNull String url, @NonNull Class<T> responseType, @NonNull Object... uriVariables) throws RestClientException {
        return retryTemplate.execute(retryContext ->
                super.getForObject(url, responseType, uriVariables));
    }

    @Override
    public <T> T getForObject(@NonNull String url, @NonNull Class<T> responseType, @NonNull Map<String, ?> uriVariables) throws RestClientException {
        return retryTemplate.execute(retryContext ->
                super.getForObject(url, responseType, uriVariables));
    }
}

