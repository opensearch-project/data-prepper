package org.opensearch.dataprepper.plugins.kafka.authenticator;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.kafka.configuration.AuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaConnectionConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.PlainTextAuthConfig;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DynamicBasicCredentialsProviderTest {
    private static final String TEST_USER = RandomStringUtils.randomAlphabetic(5);
    private static final String TEST_PASSWORD = RandomStringUtils.randomAlphabetic(5);

    @Mock
    private KafkaConnectionConfig kafkaConnectionConfig;
    @Mock
    private AuthConfig authConfig;
    @Mock
    private AuthConfig.SaslAuthConfig saslAuthConfig;
    @Mock
    private PlainTextAuthConfig plainTextAuthConfig;

    private DynamicBasicCredentialsProvider objectUnderTest;

    @BeforeEach
    void setUp() {
        objectUnderTest = new DynamicBasicCredentialsProvider();
    }

    @Test
    void testGetAfterRefreshWithBasicCredentials() {
        assertThat(objectUnderTest.getBasicCredentials(), nullValue());
        when(kafkaConnectionConfig.getAuthConfig()).thenReturn(authConfig);
        when(authConfig.getSaslAuthConfig()).thenReturn(saslAuthConfig);
        when(saslAuthConfig.getPlainTextAuthConfig()).thenReturn(plainTextAuthConfig);
        when(plainTextAuthConfig.getUsername()).thenReturn(TEST_USER);
        when(plainTextAuthConfig.getPassword()).thenReturn(TEST_PASSWORD);
        objectUnderTest.refresh(kafkaConnectionConfig);
        assertThat(objectUnderTest.getBasicCredentials(), notNullValue());
        final BasicCredentials basicCredentials = objectUnderTest.getBasicCredentials();
        assertThat(basicCredentials.getUsername(), equalTo(TEST_USER));
        assertThat(basicCredentials.getPassword(), equalTo(TEST_PASSWORD));
    }

    @Test
    void testGetAfterRefreshWithNullPlainTextAuthConfig() {
        when(kafkaConnectionConfig.getAuthConfig()).thenReturn(authConfig);
        when(authConfig.getSaslAuthConfig()).thenReturn(saslAuthConfig);
        objectUnderTest.refresh(kafkaConnectionConfig);
        assertThat(objectUnderTest.getBasicCredentials(), nullValue());
    }

    @Test
    void testGetAfterRefreshWithNullSaslAuthConfig() {
        when(kafkaConnectionConfig.getAuthConfig()).thenReturn(authConfig);
        objectUnderTest.refresh(kafkaConnectionConfig);
        assertThat(objectUnderTest.getBasicCredentials(), nullValue());
    }

    @Test
    void testGetAfterRefreshWithNullAuthConfig() {
        objectUnderTest.refresh(kafkaConnectionConfig);
        assertThat(objectUnderTest.getBasicCredentials(), nullValue());
    }
}