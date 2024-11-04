package org.opensearch.dataprepper.plugins.source.jira.rest;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.source.jira.JiraSourceConfig;
import org.opensearch.dataprepper.plugins.source.jira.rest.auth.JiraAuthConfig;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.client.InterceptingClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.BASIC;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.OAUTH2;

@ExtendWith(MockitoExtension.class)
class CustomRestTemplateConfigTest {

    private CustomRestTemplateConfig config;

    @Mock
    private JiraSourceConfig mockSourceConfig;

    @Mock
    private JiraAuthConfig mockAuthConfig;

    private static Stream<Arguments> provideAuthTypeAndExpectedInterceptorType() {
        return Stream.of(
                Arguments.of(OAUTH2, OAuth2RequestInterceptor.class),
                Arguments.of(BASIC, BasicAuthInterceptor.class),
                Arguments.of("Default", BasicAuthInterceptor.class),
                Arguments.of(null, BasicAuthInterceptor.class)
        );
    }

    @BeforeEach
    void setUp() {
        config = new CustomRestTemplateConfig();
    }

    @ParameterizedTest
    @MethodSource("provideAuthTypeAndExpectedInterceptorType")
    void testBasicAuthRestTemplateWithOAuth2(String authType, Class interceptorClassType) {
        when(mockSourceConfig.getAuthType()).thenReturn(authType);
        RestTemplate restTemplate = config.basicAuthRestTemplate(mockSourceConfig, mockAuthConfig);
        assertNotNull(restTemplate);
        assertInstanceOf(InterceptingClientHttpRequestFactory.class, restTemplate.getRequestFactory());
        List<ClientHttpRequestInterceptor> interceptors = restTemplate.getInterceptors();
        assertEquals(1, interceptors.size());
        assertInstanceOf(interceptorClassType, interceptors.get(0));
    }

}

