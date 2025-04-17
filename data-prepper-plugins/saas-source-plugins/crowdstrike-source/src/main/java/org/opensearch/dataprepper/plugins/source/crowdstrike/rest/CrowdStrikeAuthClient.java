package org.opensearch.dataprepper.plugins.source.crowdstrike.rest;

import lombok.Getter;
import org.opensearch.dataprepper.plugins.source.crowdstrike.CrowdStrikeSourceConfig;
import org.opensearch.dataprepper.plugins.source.crowdstrike.configuration.AuthenticationConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Map;
import org.springframework.http.MediaType;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.reactive.function.client.WebClient;

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
    private static final Logger log = LoggerFactory.getLogger(CrowdStrikeAuthClient.class);
    private static final String OAUTH_TOKEN_URL = "https://api.crowdstrike.com/oauth2/token";


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
        getAuthenticationToken();
    }

    /**
     * Retrieves a new authentication token from the CrowdStrike API.
     * The token is stored in the {@code bearerToken} field, and its expiration time is updated.
     *
     * @throws RuntimeException if the token cannot be retrieved.
     */
    private void getAuthenticationToken() {
        WebClient webClient = WebClient.builder().build();
        log.info(NOISY, "You are trying to access token");

        MultiValueMap<String, String> form = new LinkedMultiValueMap<>(3) {{
            add("client_id", clientId);
            add("client_secret", clientSecret);
            add("grant_type", "client_credentials");
        }};

            Map response = webClient.post()
                    .uri(OAUTH_TOKEN_URL)
                    .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                    .bodyValue(form)
                    .retrieve()
                    .bodyToMono(Map.class)
                    .block();

            if (response == null || !response.containsKey("access_token")) {
                log.error("Token response missing access_token");
                throw new RuntimeException("Token response missing access_token");
            }

            this.bearerToken = response.get("access_token").toString();
            int expiresInSeconds = (Integer) response.get("expires_in");
            this.expireTime = Instant.now().plusSeconds(expiresInSeconds);
            log.info("Access token acquired, expires in {} seconds", expiresInSeconds);
    }

    public boolean isTokenExpired() {
        return this.bearerToken == null || Instant.now().isAfter(this.expireTime);
    }

    /**
     * Refreshes the bearer token by retrieving a new one from CrowdStrike.
     */
    public void refreshToken() {
        //getAuthenticationToken();
    }
}
