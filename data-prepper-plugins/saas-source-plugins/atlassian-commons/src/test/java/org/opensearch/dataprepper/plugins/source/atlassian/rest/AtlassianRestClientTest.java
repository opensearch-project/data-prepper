/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.atlassian.rest;

import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.plugins.source.atlassian.rest.auth.AtlassianAuthConfig;
import org.opensearch.dataprepper.plugins.source.source_crawler.exception.UnauthorizedException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.atlassian.rest.AtlassianRestClient.AUTH_FAILURES_COUNTER;

@ExtendWith(MockitoExtension.class)
public class AtlassianRestClientTest {

    @Mock
    private RestTemplate restTemplate;

    @Mock
    private AtlassianAuthConfig authConfig;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private Counter authFailures;

    private static Stream<Arguments> provideHttpStatusCodesWithExceptionClass() {
        return Stream.of(
                Arguments.of(HttpStatus.FORBIDDEN, UnauthorizedException.class),
                Arguments.of(HttpStatus.UNAUTHORIZED, RuntimeException.class),
                Arguments.of(HttpStatus.TOO_MANY_REQUESTS, RuntimeException.class),
                Arguments.of(HttpStatus.INSUFFICIENT_STORAGE, RuntimeException.class)
        );
    }

    @ParameterizedTest
    @MethodSource("provideHttpStatusCodesWithExceptionClass")
    void testInvokeRestApiTokenExpired(HttpStatus statusCode, Class expectedExceptionType) {
        AtlassianRestClient atlassianRestClient = new AtlassianRestClient(restTemplate, authConfig, pluginMetrics);
        atlassianRestClient.setSleepTimeMultiplier(1);
        when(restTemplate.getForEntity(any(URI.class), any(Class.class))).thenThrow(new HttpClientErrorException(statusCode));
        URI uri = UriComponentsBuilder.fromHttpUrl("https://example.com/rest/api/2/issue/key").buildAndExpand().toUri();
        assertThrows(expectedExceptionType, () -> atlassianRestClient.invokeRestApi(uri, Object.class));
    }

    @Test
    void testAuthFailureCounter() {
        when(pluginMetrics.counter(AUTH_FAILURES_COUNTER)).thenReturn(authFailures);
        AtlassianRestClient atlassianRestClient = new AtlassianRestClient(restTemplate, authConfig, pluginMetrics);
        atlassianRestClient.setSleepTimeMultiplier(1);
        when(restTemplate.getForEntity(any(URI.class), any(Class.class))).thenThrow(new HttpClientErrorException(HttpStatus.UNAUTHORIZED));
        URI uri = UriComponentsBuilder.fromHttpUrl("https://example.com/rest/api/2/issue/key").buildAndExpand().toUri();
        assertThrows(RuntimeException.class, () -> atlassianRestClient.invokeRestApi(uri, Object.class));
        verify(authFailures, times(6)).increment();
    }


    @Test
    void testInvokeRestApiSuccessFullResponse() {
        AtlassianRestClient atlassianRestClient = new AtlassianRestClient(restTemplate, authConfig, pluginMetrics);
        atlassianRestClient.setSleepTimeMultiplier(1);
        String apiReponse = "{\"api-response\":\"ok\"}";
        ResponseEntity responseEntity = new ResponseEntity(apiReponse, HttpStatus.OK);
        when(restTemplate.getForEntity(any(URI.class), any(Class.class))).thenReturn(responseEntity);
        URI uri = UriComponentsBuilder.fromHttpUrl("https://example.com/rest/api/2/issue/key").buildAndExpand().toUri();
        assertEquals(apiReponse, atlassianRestClient.invokeRestApi(uri, String.class).getBody());
    }

}
