/*
package org.opensearch.dataprepper.plugins.source.saas.jira.rest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.web.client.DefaultResponseErrorHandler;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.List;

@Configuration
public class RestTemplateConfig {

    @Autowired
    private ClientRegistrationRepository clientRegistrationRepository;

    @Bean
    public RestTemplate restTemplate(OAuth2AuthorizedClientManager authorizedClientManager) {
        RestTemplate restTemplate = new RestTemplate();
        restTemplate.setErrorHandler(new DefaultResponseErrorHandler() {
            @Override
            protected boolean hasError(HttpStatus statusCode) {
                return statusCode.is4xxClientError() || statusCode.is5xxServerError();
            }
        });

        List<ClientHttpRequestInterceptor> interceptors = new ArrayList<>();
        interceptors.add(authenticationInterceptor(authorizedClientManager));
        restTemplate.setInterceptors(interceptors);

        return restTemplate;
    }

    @Bean
    public ClientHttpRequestInterceptor authenticationInterceptor(OAuth2AuthorizedClientManager authorizedClientManager) {
        ServletOAuth2AuthorizedClientExchangeFilterFunction oauth2 = new ServletOAuth2AuthorizedClientExchangeFilterFunction(authorizedClientManager);
        oauth2.setDefaultClientRegistrationId("my-oauth-client");

        return (request, body, execution) -> {
            if (request.getHeaders().containsKey("Authorization")) {
                return execution.execute(request, body);
            } else if (request.getHeaders().containsKey("X-Auth-Type") && request.getHeaders().get("X-Auth-Type").contains("oauth2")) {
                return oauth2.filter(request, body, execution);
            } else {
                return execution.execute(request, body);
            }
        };
    }

    @Bean
    public OAuth2AuthorizedClientManager authorizedClientManager() {
        OAuth2AuthorizedClientProvider provider = OAuth2AuthorizedClientProviderBuilder.builder()
                .clientCredentials()
                .build();

        AuthorizedClientServiceOAuth2UriClientAdapter clientAdapter = new AuthorizedClientServiceOAuth2UriClientAdapter(
                clientRegistrationRepository, provider);
        OAuth2AuthorizedClientManager manager = new OAuth2AuthorizedClientManager(clientRegistrationRepository, clientAdapter);
        return manager;
    }
}*/
