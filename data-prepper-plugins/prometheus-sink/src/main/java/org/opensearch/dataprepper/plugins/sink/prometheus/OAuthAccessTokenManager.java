/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.prometheus;

import com.github.scribejava.core.builder.api.DefaultApi20;
import com.github.scribejava.core.model.OAuth2AccessToken;
import com.github.scribejava.core.oauth.OAuth20Service;
import com.github.scribejava.core.builder.ServiceBuilder;
import org.opensearch.dataprepper.plugins.sink.prometheus.configuration.BearerTokenOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneOffset;

public class OAuthAccessTokenManager {

    private static final Logger LOG = LoggerFactory.getLogger(OAuthAccessTokenManager.class);

    public String getAccessToken(final BearerTokenOptions bearerTokenOptions) {
        OAuth20Service service = getOAuth20ServiceObj(bearerTokenOptions);
        OAuth2AccessToken accessTokenObj = null;
        try {
            if(bearerTokenOptions.getRefreshToken() != null) {
                accessTokenObj = new OAuth2AccessToken(bearerTokenOptions.getAccessToken(), bearerTokenOptions.getRefreshToken());
                accessTokenObj = service.refreshAccessToken(accessTokenObj.getRefreshToken());

            }else {
                accessTokenObj = service.getAccessTokenClientCredentialsGrant();
            }
            bearerTokenOptions.setRefreshToken(accessTokenObj.getRefreshToken());
            bearerTokenOptions.setAccessToken(accessTokenObj.getAccessToken());
            bearerTokenOptions.setTokenExpired(accessTokenObj.getExpiresIn());
        }catch (Exception e) {
            LOG.info("Exception : "+ e.getMessage() );
        }
        return bearerTokenOptions.getAccessToken();
    }


    public boolean isTokenExpired(final Integer tokenExpired){
        final Instant systemCurrentTimeStamp = Instant.now().atOffset(ZoneOffset.UTC).toInstant();
        Instant accessTokenExpTimeStamp = systemCurrentTimeStamp.plusSeconds(tokenExpired);
        if(systemCurrentTimeStamp.compareTo(accessTokenExpTimeStamp)>=0) {
            return true;
        }
        return false;
    }

    private OAuth20Service getOAuth20ServiceObj(BearerTokenOptions bearerTokenOptions){
        return  new ServiceBuilder(bearerTokenOptions.getClientId())
                .apiSecret(bearerTokenOptions.getClientSecret())
                .defaultScope(bearerTokenOptions.getScope())
                .build(new DefaultApi20() {
                    @Override
                    public String getAccessTokenEndpoint() {
                        return bearerTokenOptions.getTokenUrl();
                    }

                    @Override
                    protected String getAuthorizationBaseUrl() {
                        return bearerTokenOptions.getTokenUrl();
                    }
                });
    }

}
