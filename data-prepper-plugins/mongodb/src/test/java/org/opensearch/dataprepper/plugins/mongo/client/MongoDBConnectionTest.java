package org.opensearch.dataprepper.plugins.mongo.client;

import com.mongodb.client.MongoClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.mongo.configuration.MongoDBSourceConfig;
import org.opensearch.dataprepper.plugins.truststore.TrustStoreProvider;

import javax.net.ssl.SSLContext;
import java.nio.file.Path;
import java.util.Random;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class MongoDBConnectionTest {
    @Mock
    private MongoDBSourceConfig mongoDBSourceConfig;

    @Mock
    private MongoDBSourceConfig.AuthenticationConfig authenticationConfig;

    private final Random random = new Random();

    @BeforeEach
    void setUp() {
        when(mongoDBSourceConfig.getCredentialsConfig()).thenReturn(authenticationConfig);
        when(authenticationConfig.getUsername()).thenReturn("\uD800\uD800" + UUID.randomUUID());
        when(authenticationConfig.getPassword()).thenReturn("aЯ ⾀sd?q=%%l€0£.lo" + UUID.randomUUID());
        when(mongoDBSourceConfig.getHostname()).thenReturn(UUID.randomUUID().toString());
        when(mongoDBSourceConfig.getPort()).thenReturn(getRandomInteger());
        when(mongoDBSourceConfig.getTls()).thenReturn(getRandomBoolean());
        when(mongoDBSourceConfig.getSslInsecureDisableVerification()).thenReturn(getRandomBoolean());
        when(mongoDBSourceConfig.getReadPreference()).thenReturn("secondaryPreferred");
    }

    @Test
    public void getMongoClient() {
        final MongoClient mongoClient = MongoDBConnection.getMongoClient(mongoDBSourceConfig);
        assertThat(mongoClient, is(notNullValue()));
    }

    @Test
    public void getMongoClientWithTLS() {
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

    private Boolean getRandomBoolean() {
        return random.nextBoolean();
    }

    private int getRandomInteger() {
        return random.nextInt(10000);
    }
}
