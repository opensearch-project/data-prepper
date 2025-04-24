package org.opensearch.dataprepper.plugins.source.crowdstrike.rest;

import lombok.Getter;
import org.opensearch.dataprepper.plugins.source.crowdstrike.CrowdStrikeSourceConfig;
import org.opensearch.dataprepper.plugins.source.crowdstrike.configuration.AuthenticationConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;
import java.time.Instant;
import java.util.Map;
import javax.inject.Named;
import static org.opensearch.dataprepper.logging.DataPrepperMarkers.NOISY;


/**
 * Client to manage authentication with the CrowdStrike API.
 * Responsible for acquiring and refreshing Bearer tokens to access
 * CrowdStrike services.
 */
@Named
public class CrowdStrikeAuthClient {

    @Getter
    private String bearerToken;
    @Getter
    private Instant expireTime;
    private final String clientId;
    private final String clientSecret;
    RestTemplate restTemplate = new RestTemplate();
    private static final Logger log = LoggerFactory.getLogger(CrowdStrikeAuthClient.class);
    private static final String OAUTH_TOKEN_URL = "https://api.crowdstrike.com/oauth2/token";
    private static final String ACCESS_TOKEN = "access_token";
    private static final String EXPIRE_IN = "expires_in";



    public CrowdStrikeAuthClient(final CrowdStrikeSourceConfig sourceConfig) {
        AuthenticationConfig authConfig = sourceConfig.getAuthenticationConfig();
        this.clientId = authConfig.getClientId();
        this.clientSecret = authConfig.getClientSecret();
    }


    /**
     * Initializes the credentials by obtaining an authentication token.
     */
    public void initCredentials() {
        log.info("Getting CrowdStrike Authentication Token");
        getAuthToken();
    }

    /**
     * Retrieves a new authentication token from the CrowdStrike API.
     * The token is stored in the {@code bearerToken} field, and its expiration time is updated.
     *
     * @throws RuntimeException if the token cannot be retrieved.
     */
    private void getAuthToken() {
        log.info(NOISY, "You are trying to access token");
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
        headers.setBasicAuth(this.clientId, this.clientSecret);
        HttpEntity<String> request = new HttpEntity<>(headers);
        try {
            ResponseEntity<Map> response = restTemplate.postForEntity(OAUTH_TOKEN_URL, request, Map.class);
            Map tokenData = response.getBody();
            this.bearerToken = (String) tokenData.get(ACCESS_TOKEN);
            this.expireTime = Instant.now().plusSeconds((Integer) tokenData.get(EXPIRE_IN));
            log.info("Access token acquired successfully");
        } catch (HttpClientErrorException ex) {
            this.expireTime = Instant.ofEpochMilli(0);
            HttpStatus statusCode = ex.getStatusCode();
            log.error("Failed to acquire access token. Status code: {}, Error Message: {}",
                    statusCode, ex.getMessage());
            throw new RuntimeException("Error while requesting token:" + ex.getMessage(), ex);
        }
    }

    public boolean isTokenExpired() {
        return this.bearerToken == null || Instant.now().isAfter(this.expireTime);
    }

    /**
     * Refreshes the bearer token by retrieving a new one from CrowdStrike.
     */
    public void refreshToken() {

    }
}
