package org.opensearch.dataprepper.plugins.source.jira.rest;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.source.jira.rest.auth.JiraOauthConfig;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpRequest;
import org.springframework.http.client.ClientHttpRequestExecution;
import org.springframework.http.client.ClientHttpResponse;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class OAuth2RequestInterceptorTest {

    @Mock
    private HttpRequest mockRequest;

    @Mock
    private ClientHttpRequestExecution mockExecution;

    @Mock
    private ClientHttpResponse mockResponse;

    @Mock
    private JiraOauthConfig mockConfig;

    @Mock
    private HttpHeaders mockHeaders;

    private OAuth2RequestInterceptor interceptor;

    @BeforeEach
    void setUp() {
        when(mockConfig.getAccessToken()).thenReturn("testAccessToken");
        when(mockRequest.getHeaders()).thenReturn(mockHeaders);
        interceptor = new OAuth2RequestInterceptor(mockConfig);
    }


    @Test
    void testInterceptAddsAuthorizationHeader() throws IOException {
        when(mockExecution.execute(any(HttpRequest.class), any(byte[].class))).thenReturn(mockResponse);
        ClientHttpResponse response = interceptor.intercept(mockRequest, new byte[0], mockExecution);
        verify(mockHeaders).setBearerAuth("testAccessToken");
        assertEquals(mockResponse, response);
    }
}
