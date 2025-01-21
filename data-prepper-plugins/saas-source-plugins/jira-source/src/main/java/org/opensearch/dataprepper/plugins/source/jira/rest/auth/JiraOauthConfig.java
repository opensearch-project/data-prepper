/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.jira.rest.auth;

import lombok.Getter;
import org.opensearch.dataprepper.plugins.source.jira.JiraSourceConfig;
import org.opensearch.dataprepper.plugins.source.jira.exception.UnAuthorizedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.RETRY_ATTEMPT;
import static org.opensearch.dataprepper.plugins.source.jira.utils.JqlConstants.SLASH;

/**
 * The type Jira service.
 */

public class JiraOauthConfig implements JiraAuthConfig {

    public static final String OAuth2_URL = "https://api.atlassian.com/ex/jira/";
    public static final String ACCESSIBLE_RESOURCES = "https://api.atlassian.com/oauth/token/accessible-resources";
    public static final String TOKEN_LOCATION = "https://auth.atlassian.com/oauth/token";

    public static final String EXPIRES_IN = "expires_in";
    public static final String REFRESH_TOKEN = "refresh_token";
    public static final String ACCESS_TOKEN = "access_token";
    private static final Logger log = LoggerFactory.getLogger(JiraOauthConfig.class);
    private final String clientId;
    private final String clientSecret;
    private final JiraSourceConfig jiraSourceConfig;
    private final Object cloudIdFetchLock = new Object();
    private final Object tokenRenewLock = new Object();
    RestTemplate restTemplate = new RestTemplate();
    private String url;
    @Getter
    private int expiresInSeconds = 0;
    @Getter
    private Instant expireTime = Instant.ofEpochMilli(0);
    @Getter
    private String accessToken;
    @Getter
    private String refreshToken;
    private String cloudId = null;

    public JiraOauthConfig(JiraSourceConfig jiraSourceConfig) {
        this.jiraSourceConfig = jiraSourceConfig;
        this.accessToken = (String) jiraSourceConfig.getAuthenticationConfig().getOauth2Config()
                .getAccessToken().getValue();
        this.refreshToken = (String) jiraSourceConfig.getAuthenticationConfig()
                .getOauth2Config().getRefreshToken().getValue();
        this.clientId = jiraSourceConfig.getAuthenticationConfig().getOauth2Config().getClientId();
        this.clientSecret = jiraSourceConfig.getAuthenticationConfig().getOauth2Config().getClientSecret();
    }

    public String getJiraAccountCloudId() {
        log.info("Getting Jira Account Cloud ID");
        synchronized (cloudIdFetchLock) {
            if (this.cloudId != null) {
                //Someone else must have initialized it
                return this.cloudId;
            }

            int retryCount = 0;
            while (retryCount < RETRY_ATTEMPT) {
                retryCount++;
                try {
                    HttpHeaders headers = new HttpHeaders();
                    headers.setBearerAuth(accessToken);
                    headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
                    HttpEntity<Void> entity = new HttpEntity<>(headers);
                    ResponseEntity<Object> exchangeResponse =
                            restTemplate.exchange(ACCESSIBLE_RESOURCES, HttpMethod.GET, entity, Object.class);
                    List<Map<String, Object>> listResponse = (List<Map<String, Object>>) exchangeResponse.getBody();
                    Map<String, Object> response = listResponse.get(0);
                    this.cloudId = (String) response.get("id");
                    return this.cloudId;
                } catch (HttpClientErrorException e) {
                    if (e.getRawStatusCode() == HttpStatus.UNAUTHORIZED.value()) {
                        renewCredentials();
                    }
                    log.error("Error occurred while accessing resources: ", e);
                }
            }
            throw new UnAuthorizedException(String.format("Access token expired. Unable to renew even after %s attempts", RETRY_ATTEMPT));
        }
    }

    public void renewCredentials() {
        Instant currentTime = Instant.now();
        if (expireTime.isAfter(currentTime)) {
            //There is still time to renew or someone else must have already renewed it
            return;
        }

        synchronized (tokenRenewLock) {
            if (expireTime.isAfter(currentTime)) {
                //Someone else must have already renewed it
                return;
            }

            log.info("Renewing access token and refresh token pair for Jira Connector.");
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            String payloadTemplate = "{\"grant_type\": \"%s\", \"client_id\": \"%s\", \"client_secret\": \"%s\", \"refresh_token\": \"%s\"}";
            String payload = String.format(payloadTemplate, "refresh_token", clientId, clientSecret, refreshToken);
            HttpEntity<String> entity = new HttpEntity<>(payload, headers);

            try {
                ResponseEntity<Map> responseEntity = restTemplate.postForEntity(TOKEN_LOCATION, entity, Map.class);
                Map<String, Object> oauthClientResponse = responseEntity.getBody();
                this.accessToken = (String) oauthClientResponse.get(ACCESS_TOKEN);
                this.refreshToken = (String) oauthClientResponse.get(REFRESH_TOKEN);
                this.expiresInSeconds = (int) oauthClientResponse.get(EXPIRES_IN);
                this.expireTime = Instant.ofEpochMilli(System.currentTimeMillis() + (expiresInSeconds * 1000L));
                // updating config object's PluginConfigVariable so that it updates the underlying Secret store
                jiraSourceConfig.getAuthenticationConfig().getOauth2Config().getAccessToken()
                        .setValue(this.accessToken);
                jiraSourceConfig.getAuthenticationConfig().getOauth2Config().getRefreshToken()
                        .setValue(this.refreshToken);
                log.info("Access Token and Refresh Token pair is now refreshed. Corresponding Secret store key updated.");
            } catch (HttpClientErrorException ex) {
                this.expireTime = Instant.ofEpochMilli(0);
                this.expiresInSeconds = 0;
                // Try refreshing the secrets and see if that helps
                // Refreshing one of the secret refreshes the entire store so we are good to trigger refresh on just one
                jiraSourceConfig.getAuthenticationConfig().getOauth2Config().getAccessToken().refresh();
                log.error("Failed to renew access token. Status code: {}, Error Message: {}",
                        ex.getRawStatusCode(), ex.getMessage());
                throw new RuntimeException("Failed to renew access token" + ex.getMessage(), ex);
            }
        }
    }

    @Override
    public String getUrl() {
        if (!StringUtils.hasLength(url)) {
            synchronized (cloudIdFetchLock) {
                if (!StringUtils.hasLength(url)) {
                    initCredentials();
                }
            }
        }
        return url;
    }

    /**
     * Method for getting Jira url based on auth type.
     */
    @Override
    public void initCredentials() {
        //For OAuth based flow, we use a different Jira url
        this.cloudId = getJiraAccountCloudId();
        this.url = OAuth2_URL + this.cloudId + SLASH;
    }
}
