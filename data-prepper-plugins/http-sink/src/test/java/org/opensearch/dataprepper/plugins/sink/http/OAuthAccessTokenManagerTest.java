/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.http;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.HttpEntity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.opensearch.dataprepper.plugins.sink.http.configuration.BearerTokenOptions;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OAuthAccessTokenManagerTest {

    private static final String bearerTokenYaml =
            "            client_id: 0oaafr4j79segrYGC5d7\n" +
            "            client_secret: fFel-3FutCXAOndezEsOVlght6D6DR4OIt7G5D1_oJ6w0wNoaYtgU17JdyXmGf0M\n" +
            "            token_url: https://localhost/oauth2/default/v1/token\n" +
            "            grant_type: client_credentials\n" +
            "            scope: httpSink";

    private final String tokenJson = "{\"token_type\": \"Bearer\",\n" +
            "    \"expires_in\": 3600,\n" +
            "    \"access_token\": \"eyJraWQiOiJtU0xMalBfMUFFUFV1VzlqRkhiSmc4UXlRSm1pdFBHamZOczR2eFJ2WUx3IiwiYWxnIjoiUlMyNTYifQ.eyJ2ZXIiOjEsImp0aSI6IkFULm1jcVVfTzJvd1RuNUwwTjYwLTg0WFBaUU4xNXBfX191X2VKaGxBdEhsQXciLCJpc3MiOiJodHRwczovL2Rldi03NTA1MDk1Ni5va3RhLmNvbS9vYXV0aDIvZGVmYXVsdCIsImF1ZCI6ImFwaTovL2RlZmF1bHQiLCJpYXQiOjE2ODk5MTQyNDMsImV4cCI6MTY4OTkxNzg0MywiY2lkIjoiMG9hYWZyNGo3OXNlZ3JZR0M1ZDciLCJzY3AiOlsiaHR0cFNpbmsiXSwic3ViIjoiMG9hYWZyNGo3OXNlZ3JZR0M1ZDcifQ.d6NU2f9Qlrk9N2L1cfY5KhWIc7DXE1oJXPsss2OMb-JFYZvwhMpIpv1IwaY7ikDQYDKlcYYt-3XKBj0IxPnugigO_OTv12LpvHyMBhUKDo5YrxKZqksme7S0IKYoLNFVsq3ViqVsHgDy3RGWL1ih-rGXN-8A-9LsqloEnCn7SzFj446aep9bygp1PIA5pBgrVwKw0QPal4HDOu9cTKwclNiWRLJ80H_q83vDeQNnW9YI8A-nTy9ujghVF9JJVsB4FTHMlfclt93SJ4qCA_9He_VFkSs5pFS4plCAzONA0XU53lf7NXJ3bs18HPJkm3-B2b1f6Q9kGUU6e2ZQ2d6dvw\",\n" +
            "    \"scope\": \"httpSink\"\n" +
            "}";

    private ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.USE_PLATFORM_LINE_BREAKS));

    @Mock
    private HttpClientBuilder httpClientBuilder;

    @Mock
    private CloseableHttpClient httpClient;

    private OAuthAccessTokenManager oAuthAccessTokenManager;

    @BeforeEach
    public void setup() throws IOException {
        httpClientBuilder = mock(HttpClientBuilder.class);
        httpClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse response = mock(CloseableHttpResponse.class);
        HttpEntity entity = mock(HttpEntity.class);

        final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(tokenJson.getBytes());
        when(entity.getContent()).thenReturn(byteArrayInputStream);
        when(httpClientBuilder.build()).thenReturn(httpClient);
        when(httpClient.execute(any(ClassicHttpRequest.class))).thenReturn(response);
        when(response.getEntity()).thenReturn(entity);
        this.oAuthAccessTokenManager = new OAuthAccessTokenManager(httpClientBuilder);
    }

    @Test
    public void bearer_token_refresh_token_test() throws IOException {
        BearerTokenOptions bearerTokenOptions = objectMapper.readValue(bearerTokenYaml,BearerTokenOptions.class);
        final String refreshToken = oAuthAccessTokenManager.getAccessToken(bearerTokenOptions);
        assertThat(refreshToken,equalTo("Bearer eyJraWQiOiJtU0xMalBfMUFFUFV1VzlqRkhiSmc4UXlRSm1pdFBHamZOczR2eFJ2WUx3IiwiYWxnIjoiUlMyNTYifQ.eyJ2ZXIiOjEsImp0aSI6IkFULm1jcVVfTzJvd1RuNUwwTjYwLTg0WFBaUU4xNXBfX191X2VKaGxBdEhsQXciLCJpc3MiOiJodHRwczovL2Rldi03NTA1MDk1Ni5va3RhLmNvbS9vYXV0aDIvZGVmYXVsdCIsImF1ZCI6ImFwaTovL2RlZmF1bHQiLCJpYXQiOjE2ODk5MTQyNDMsImV4cCI6MTY4OTkxNzg0MywiY2lkIjoiMG9hYWZyNGo3OXNlZ3JZR0M1ZDciLCJzY3AiOlsiaHR0cFNpbmsiXSwic3ViIjoiMG9hYWZyNGo3OXNlZ3JZR0M1ZDcifQ.d6NU2f9Qlrk9N2L1cfY5KhWIc7DXE1oJXPsss2OMb-JFYZvwhMpIpv1IwaY7ikDQYDKlcYYt-3XKBj0IxPnugigO_OTv12LpvHyMBhUKDo5YrxKZqksme7S0IKYoLNFVsq3ViqVsHgDy3RGWL1ih-rGXN-8A-9LsqloEnCn7SzFj446aep9bygp1PIA5pBgrVwKw0QPal4HDOu9cTKwclNiWRLJ80H_q83vDeQNnW9YI8A-nTy9ujghVF9JJVsB4FTHMlfclt93SJ4qCA_9He_VFkSs5pFS4plCAzONA0XU53lf7NXJ3bs18HPJkm3-B2b1f6Q9kGUU6e2ZQ2d6dvw"));
    }

    @Test
    public void bearer_token_refresh_token_expiry_test() throws IOException {
        String bearerToken = "\"Bearer eyJraWQiOiJtU0xMalBfMUFFUFV1VzlqRkhiSmc4UXlRSm1pdFBHamZOczR2eFJ2WUx3IiwiYWxnIjoiUlMyNTYifQ.eyJ2ZXIiOjEsImp0aSI6IkFULm1jcVVfTzJvd1RuNUwwTjYwLTg0WFBaUU4xNXBfX191X2VKaGxBdEhsQXciLCJpc3MiOiJodHRwczovL2Rldi03NTA1MDk1Ni5va3RhLmNvbS9vYXV0aDIvZGVmYXVsdCIsImF1ZCI6ImFwaTovL2RlZmF1bHQiLCJpYXQiOjE2ODk5MTQyNDMsImV4cCI6MTY4OTkxNzg0MywiY2lkIjoiMG9hYWZyNGo3OXNlZ3JZR0M1ZDciLCJzY3AiOlsiaHR0cFNpbmsiXSwic3ViIjoiMG9hYWZyNGo3OXNlZ3JZR0M1ZDcifQ.d6NU2f9Qlrk9N2L1cfY5KhWIc7DXE1oJXPsss2OMb-JFYZvwhMpIpv1IwaY7ikDQYDKlcYYt-3XKBj0IxPnugigO_OTv12LpvHyMBhUKDo5YrxKZqksme7S0IKYoLNFVsq3ViqVsHgDy3RGWL1ih-rGXN-8A-9LsqloEnCn7SzFj446aep9bygp1PIA5pBgrVwKw0QPal4HDOu9cTKwclNiWRLJ80H_q83vDeQNnW9YI8A-nTy9ujghVF9JJVsB4FTHMlfclt93SJ4qCA_9He_VFkSs5pFS4plCAzONA0XU53lf7NXJ3bs18HPJkm3-B2b1f6Q9kGUU6e2ZQ2d6dvw\"";
        final boolean refreshToken = oAuthAccessTokenManager.isTokenExpired(bearerToken);
        assertThat(refreshToken,equalTo(Boolean.TRUE));
    }
}
