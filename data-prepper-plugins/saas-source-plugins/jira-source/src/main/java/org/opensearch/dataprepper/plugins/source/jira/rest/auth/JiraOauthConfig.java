package org.opensearch.dataprepper.plugins.source.jira.rest.auth;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import org.opensearch.dataprepper.plugins.source.jira.JiraSourceConfig;
import org.opensearch.dataprepper.plugins.source.jira.exception.UnAuthorizedException;
import org.opensearch.dataprepper.plugins.source.jira.utils.Constants;
import org.slf4j.Logger;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.ACCESSIBLE_RESOURCES;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.AUTHORIZATION_ERROR_CODE;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.OAuth2_URL;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.RETRY_ATTEMPT;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.SLASH;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.SUCCESS_RESPONSE;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.TOKEN_EXPIRED;

/**
 * The type Jira service.
 */

@Getter
public class JiraOauthConfig implements JiraAuthConfig {

    private static final Logger log =
            org.slf4j.LoggerFactory.getLogger(JiraOauthConfig.class);

    private final String clientId;
    private final String clientSecret;
    private final JiraSourceConfig jiraSourceConfig;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Object cloudIdFetchLock = new Object();
    private final Object tokenRenewLock = new Object();
    RestTemplate restTemplate = new RestTemplate();
    private int expiresInSeconds = 0;
    private Instant expireTime;
    private String accessToken;
    private String refreshToken;
    private String url;
    private String cloudId = null;

    public JiraOauthConfig(JiraSourceConfig jiraSourceConfig) {
        this.jiraSourceConfig = jiraSourceConfig;
        this.accessToken = jiraSourceConfig.getAccessToken();
        this.refreshToken = jiraSourceConfig.getRefreshToken();
        this.clientId = jiraSourceConfig.getClientId();
        this.clientSecret = jiraSourceConfig.getClientSecret();
    }

    private String getJiraAccountCloudId() {
        log.info("Getting Jira Account Cloud ID");
        if (this.cloudId != null) {
            return this.cloudId;
        }
        synchronized (cloudIdFetchLock) {
            if (this.cloudId != null) {
                //Someone else must have initialized it
                return this.cloudId;
            }

            HttpHeaders headers = new HttpHeaders();
            headers.setBearerAuth(jiraSourceConfig.getAccessToken());
            headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
            HttpEntity<Void> entity = new HttpEntity<>(headers);
            int retryCount = 0;
            while (retryCount < RETRY_ATTEMPT) {
                retryCount++;
                try {
                    ResponseEntity<Object> exchangeResponse =
                            restTemplate.exchange(ACCESSIBLE_RESOURCES, HttpMethod.GET, entity, Object.class);
                    List<Map<String, Object>> listResponse = (List<Map<String, Object>>) exchangeResponse.getBody();
                    Map<String, Object> response = listResponse.get(0);
                    return (String) response.get("id");
                } catch (HttpClientErrorException e) {
                    if (e.getStatusCode().value() == TOKEN_EXPIRED) {
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
        if (expireTime != null && expireTime.isAfter(currentTime)) {
            //There is still time to renew or someone else must have already renewed it
            return;
        }

        synchronized (tokenRenewLock) {
            if (expireTime != null && expireTime.isAfter(currentTime)) {
                //Someone else must have already renewed it
                return;
            }

            log.info("Renewing access-refresh token pair for Jira Connector.");

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            Map<String, String> payloadMap =
                    Map.of("grant_type", "refresh_token",
                            "client_id", clientId,
                            "client_secret", clientSecret,
                            "refresh_token", refreshToken);
            String payload;
            try {
                payload = objectMapper.writeValueAsString(payloadMap);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
            HttpEntity<String> entity = new HttpEntity<>(payload, headers);

            ResponseEntity<Map> responseEntity = restTemplate.postForEntity(
                    Constants.TOKEN_LOCATION,
                    entity,
                    Map.class
            );
            int statusCode = responseEntity.getStatusCode().value();
            if (statusCode == AUTHORIZATION_ERROR_CODE) {
                log.error("Authorization Exception occurred while renewing access token {} ", responseEntity.getBody());
            } else if (statusCode == SUCCESS_RESPONSE) {

                Map<String, Object> oauthClientResponse = responseEntity.getBody();
                String newAccessToken = (String) oauthClientResponse.get(Constants.ACCESS_TOKEN);
                String newRefreshToken = (String) oauthClientResponse.get(Constants.REFRESH_TOKEN);

                if (!StringUtils.hasLength(newAccessToken)) {
                    log.debug("Access token is empty or null");
                    throw new RuntimeException("Access token is empty or null");
                }
                if (!StringUtils.hasLength(newRefreshToken)) {
                    log.debug("Refresh token is empty or null ");
                    throw new RuntimeException("Refresh token is empty or null");
                }

                this.accessToken = newAccessToken;
                this.refreshToken = newRefreshToken;
                this.expiresInSeconds = (int) oauthClientResponse.get(Constants.EXPIRES_IN);
                this.expireTime = Instant.ofEpochMilli(System.currentTimeMillis() + (expiresInSeconds * 1000L));
            } else {
                throw new RuntimeException("Failed to renew access token" + responseEntity.getBody());
            }
        }
    }

    @Override
    public String getUrl() {
        if (url == null || url.isEmpty()) {
            synchronized (cloudIdFetchLock) {
                if (url == null || url.isEmpty()) {
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
