package org.opensearch.dataprepper.plugins.mongo.client;

import com.mongodb.client.MongoClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.mongo.configuration.AwsConfig;
import org.opensearch.dataprepper.plugins.mongo.configuration.MongoDBSourceConfig;
import org.opensearch.dataprepper.plugins.truststore.TrustStoreProvider;

import javax.net.ssl.SSLContext;
import java.nio.file.Path;
import java.util.Random;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class MongoDBConnectionTest {
    @Mock
    private MongoDBSourceConfig mongoDBSourceConfig;

    @Mock
    private MongoDBSourceConfig.AuthenticationConfig authenticationConfig;

    @Mock
    private AwsConfig awsConfig;

    private final Random random = new Random();

    void setUp() {
        when(mongoDBSourceConfig.getAuthenticationConfig()).thenReturn(authenticationConfig);
        when(mongoDBSourceConfig.getHost()).thenReturn(UUID.randomUUID().toString());
        when(mongoDBSourceConfig.getPort()).thenReturn(getRandomInteger());
        when(mongoDBSourceConfig.getTls()).thenReturn(getRandomBoolean());
        when(mongoDBSourceConfig.getSslInsecureDisableVerification()).thenReturn(getRandomBoolean());
        when(mongoDBSourceConfig.getReadPreference()).thenReturn("secondaryPreferred");
    }

    @Test
    public void getMongoClientWithUsernamePassword() {
        setUp();
        when(authenticationConfig.getUsername()).thenReturn("\uD800\uD800" + UUID.randomUUID());
        when(authenticationConfig.getPassword()).thenReturn("aЯ ⾀sd?q=%%l€0£.lo" + UUID.randomUUID());
        final MongoClient mongoClient = MongoDBConnection.getMongoClient(mongoDBSourceConfig);
        assertThat(mongoClient, is(notNullValue()));
    }

    @Test
    public void getMongoClientWithTLS() {
        setUp();
        when(authenticationConfig.getUsername()).thenReturn("\uD800\uD800" + UUID.randomUUID());
        when(authenticationConfig.getPassword()).thenReturn("aЯ ⾀sd?q=%%l€0£.lo" + UUID.randomUUID());
        when(mongoDBSourceConfig.getTrustStoreFilePath()).thenReturn(UUID.randomUUID().toString());
        when(mongoDBSourceConfig.getTrustStorePassword()).thenReturn(UUID.randomUUID().toString());
        final Path path = mock(Path.class);
        final SSLContext sslContext = mock(SSLContext.class);
        try (MockedStatic<TrustStoreProvider> trustStoreProviderMockedStatic = mockStatic(TrustStoreProvider.class)) {
            trustStoreProviderMockedStatic.when(() -> TrustStoreProvider.createSSLContext(path,
                            UUID.randomUUID().toString()))
                    .thenReturn(sslContext);
            final MongoClient mongoClient = MongoDBConnection.getMongoClient(mongoDBSourceConfig);
            assertThat(mongoClient, is(notNullValue()));
        }
    }

    @Test
    public void getMongoClientNullHost() {
        when(mongoDBSourceConfig.getHost()).thenReturn(null);
        assertThrows(RuntimeException.class, () -> MongoDBConnection.getMongoClient(mongoDBSourceConfig));
    }

    @Test
    public void getMongoClientEmptyHost() {
        when(mongoDBSourceConfig.getHost()).thenReturn(" ");
        assertThrows(RuntimeException.class, () -> MongoDBConnection.getMongoClient(mongoDBSourceConfig));
    }

    @Test
    public void getMongoClientWithIAMAuth() {
        setUp();
        when(mongoDBSourceConfig.getAuthenticationConfig()).thenReturn(null);
        when(mongoDBSourceConfig.getAwsConfig()).thenReturn(awsConfig);
        when(awsConfig.getAwsStsRoleArn()).thenReturn("arn:aws:iam::123456789012:role/testRole");
        final MongoClient mongoClient = MongoDBConnection.getMongoClient(mongoDBSourceConfig);
        assertThat(mongoClient, is(notNullValue()));
    }

    private Boolean getRandomBoolean() {
        return random.nextBoolean();
    }

    private int getRandomInteger() {
        return random.nextInt(10000);
    }
}
