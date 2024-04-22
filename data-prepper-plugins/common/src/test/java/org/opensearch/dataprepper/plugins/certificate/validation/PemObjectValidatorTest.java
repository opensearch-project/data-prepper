package org.opensearch.dataprepper.plugins.certificate.validation;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

class PemObjectValidatorTest {
    private static final String TEST_CERTIFICATE = "-----BEGIN CERTIFICATE-----\n" +
            "MIIDYTCCAkmgAwIBAgIUFBUALAhfpezYbYw6AtH96tizPTIwDQYJKoZIhvcNAQEL\n" +
            "BQAwWTELMAkGA1UEBhMCQVUxEzARBgNVBAgMClNvbWUtU3RhdGUxITAfBgNVBAoM\n" +
            "GEludGVybmV0IFdpZGdpdHMgUHR5IEx0ZDESMBAGA1UEAwwJMTI3LjAuMC4xMB4X\n" +
            "DTI0MDQxMTE3MzcyMloXDTM0MDQwOTE3MzcyMlowWTELMAkGA1UEBhMCQVUxEzAR\n" +
            "BgNVBAgMClNvbWUtU3RhdGUxITAfBgNVBAoMGEludGVybmV0IFdpZGdpdHMgUHR5\n" +
            "IEx0ZDESMBAGA1UEAwwJMTI3LjAuMC4xMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8A\n" +
            "MIIBCgKCAQEAuaS6lrpg38XT5wmukekr8NSXcO70yhMRLF29mAXasYeumtHVDR/p\n" +
            "f8vTE4l+b36kRuaew4htGRZQcWJBdPoCDkDHA3+5z5t9Fe3nR9FzIA+E/KjyMCEq\n" +
            "xNgc9OIN9UyBbbneMkR24W8LkAywxk3euXgj46+7SGFHAdNLqC72Yl3W1E32rQAR\n" +
            "c6zQIJ45uogqU19QJHCBBfJA+IFylwtNGWfNbvdvGCXx5FZnM3q4rCxNr9F+LBsS\n" +
            "xWFlXGMHXo2+bMBGIBXPGbGXpad/jVgxjM6zV5vnG1g8GDxaHaM+3LjJwa7eQYVA\n" +
            "ogetug9wqesxkf+Nic/rpB6J7zM2iwY0AQIDAQABoyEwHzAdBgNVHQ4EFgQUept4\n" +
            "OD2pNRYswtfrOqnOgx4QtjYwDQYJKoZIhvcNAQELBQADggEBACU+Qjmf35BOYjDj\n" +
            "TX1f6bhgwsHP/WHwWWKIhVSOB0CFHoizzQyLREgWWLkKs3Ye3q9DXku0saMfIerq\n" +
            "S7hqDA8hNVJVyllh2FuuNQVkmOKJFTwev2n3OvSyz4mxWW3UNJb/YTuK93nNHVVo\n" +
            "/3+lQg0sJRhSMs/GmR/Hn7/py2/2pucFJrML/Dtjv7SwrOXptWn+GCB+3bUpfNdg\n" +
            "sHeZEv9vpbQDzp1Lux7l3pMzwsi6HU4xTyHClBD7V8S2MUExMXDF+Cr4g7lmOb02\n" +
            "Bw0dTI7afBMI8n5YgTX78YMGqbO/WJ3bOc26P2i7RrRIhOXw69UZff2JwYAnX6Op\n" +
            "zHOodz4=\n" +
            "-----END CERTIFICATE-----";

    @Test
    void testValidPemObject() {
        assertThat(PemObjectValidator.isPemObject(TEST_CERTIFICATE), is(true));
    }

    @Test
    void testInValidPemObject() {
        assertThat(PemObjectValidator.isPemObject(UUID.randomUUID().toString()), is(false));
    }
}