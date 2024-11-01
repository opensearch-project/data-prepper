package org.opensearch.dataprepper.plugins.source.jira.rest;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.source.jira.JiraSourceConfig;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpRequest;
import org.springframework.http.client.ClientHttpRequestExecution;
import org.springframework.http.client.ClientHttpResponse;

import java.io.IOException;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class BasicAuthInterceptorTest {

    @Mock
    private HttpRequest mockRequest;

    @Mock
    private ClientHttpRequestExecution mockExecution;

    @Mock
    private ClientHttpResponse mockResponse;

    @Mock
    private JiraSourceConfig mockConfig;

    @Mock
    private HttpHeaders mockHeaders;

    private BasicAuthInterceptor interceptor;

    @BeforeEach
    void setUp() {
        when(mockConfig.getJiraId()).thenReturn("testUser");
        when(mockConfig.getJiraCredential()).thenReturn("testPassword");
        when(mockRequest.getHeaders()).thenReturn(mockHeaders);
        interceptor = new BasicAuthInterceptor(mockConfig);
    }

    @Test
    void testInterceptAddsAuthorizationHeader() throws IOException {
        when(mockExecution.execute(any(HttpRequest.class), any(byte[].class))).thenReturn(mockResponse);

        ClientHttpResponse response = interceptor.intercept(mockRequest, new byte[0], mockExecution);

        verify(mockHeaders).set(eq(HttpHeaders.AUTHORIZATION), argThat(value ->
                value.startsWith("Basic ") &&
                        new String(Base64.getDecoder().decode(value.substring(6))).equals("testUser:testPassword")
        ));
        assertEquals(mockResponse, response);
    }

}
