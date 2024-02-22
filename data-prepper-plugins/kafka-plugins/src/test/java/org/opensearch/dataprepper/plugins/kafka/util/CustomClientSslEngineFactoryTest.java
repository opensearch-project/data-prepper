package org.opensearch.dataprepper.plugins.kafka.util;

import org.junit.jupiter.api.Test;
import javax.net.ssl.SSLEngine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Objects;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;

public class CustomClientSslEngineFactoryTest {
    @Test
    public void createClientSslEngine() {
        try (final CustomClientSslEngineFactory customClientSslEngineFactory = new CustomClientSslEngineFactory()) {
            final SSLEngine sslEngine = customClientSslEngineFactory.createClientSslEngine(anyString(), anyInt(), anyString());
            assertThat(sslEngine, is(notNullValue()));
        }
    }

    @Test
    public void createClientSslEngineWithConfig() throws IOException {
        try (final CustomClientSslEngineFactory customClientSslEngineFactory = new CustomClientSslEngineFactory()) {
                final Path certFilePath = Path.of(Objects.requireNonNull(getClass().getClassLoader()
                    .getResource("test_cert.crt")).getPath());

            final String certificateContent = Files.readString(certFilePath);
            customClientSslEngineFactory.configure(Collections.singletonMap("certificateContent", certificateContent));
            final SSLEngine sslEngine = customClientSslEngineFactory.createClientSslEngine(anyString(), anyInt(), anyString());
            assertThat(sslEngine, is(notNullValue()));
        }
    }
}
