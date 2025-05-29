/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.microsoft_office365.auth;

import lombok.Getter;
import org.opensearch.dataprepper.plugins.source.microsoft_office365.Office365SourceConfig;
import org.opensearch.dataprepper.plugins.source.microsoft_office365.RetryHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.time.Instant;
import java.util.Map;

/**
 * OAuth2 implementation of the Office365AuthProvider.
 */
@Component
public class Office365AuthenticationProvider implements Office365AuthenticationInterface {
    private static final Logger log = LoggerFactory.getLogger(Office365AuthenticationProvider.class);
    private static final String TOKEN_URL = "https://login.microsoftonline.com/%s/oauth2/v2.0/token";
    private static final String MANAGEMENT_API_SCOPE = "https://manage.office.com/.default";
    private static final String ACCESS_TOKEN_REQUEST_BODY = "grant_type=client_credentials" +
            "&client_id=%s" +
            "&client_secret=%s" +
            "&scope=%s";

    private final RestTemplate restTemplate = new RestTemplate();
    private final String clientId;
    private final String clientSecret;
    private final String tenantId;
    private final Object lock = new Object();

    @Getter
    private String accessToken;
    private Instant expireTime = Instant.ofEpochMilli(0);

    public Office365AuthenticationProvider(Office365SourceConfig config) {
        this.clientId = config.getAuthenticationConfiguration().getOauth2().getClientId();
        this.clientSecret = config.getAuthenticationConfiguration().getOauth2().getClientSecret();
        this.tenantId = config.getTenantId();
    }

    @Override
    public String getTenantId() {
        return this.tenantId;
    }

    @Override
    public void initCredentials() {
        log.info("Initializing credentials.");
        renewCredentials();
    }

    @Override
    public void renewCredentials() {
        synchronized(lock) {
            log.info("Getting new access token for Office 365 Management API");

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);

            String payload = String.format(ACCESS_TOKEN_REQUEST_BODY, clientId, clientSecret, MANAGEMENT_API_SCOPE);

            HttpEntity<String> entity = new HttpEntity<>(payload, headers);
            String tokenEndpoint = String.format(TOKEN_URL, tenantId);

            ResponseEntity<Map> response = RetryHandler.executeWithRetry(
                    () -> restTemplate.postForEntity(tokenEndpoint, entity, Map.class),
                    () -> {
                    } // No credential renewal for authentication endpoint
            );

            Map<String, Object> tokenResponse = response.getBody();

            if (tokenResponse == null || tokenResponse.get("access_token") == null) {
                throw new IllegalStateException("Invalid token response: missing access_token");
            }

            this.accessToken = (String) tokenResponse.get("access_token");
            int expiresIn = (int) tokenResponse.get("expires_in");
            this.expireTime = Instant.now().plusSeconds(expiresIn);
            log.info("Received new access token. Expires in {} seconds", expiresIn);
        }
    }
}
