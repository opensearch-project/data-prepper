package org.opensearch.dataprepper.plugins.sink.prometheus.util;

import org.apache.hc.core5.http.HttpHost;
import org.junit.jupiter.api.Test;

import java.net.MalformedURLException;
import java.net.URL;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class PrometheusSinkUtilTest {

    @Test
    public void get_url_by_url_string_positive_test() throws MalformedURLException {
        assertEquals(PrometheusSinkUtil.getURLByUrlString("http://localhost:8080"), new URL("http://localhost:8080"));
    }

    @Test
    public void get_http_host_by_url_positive_test() throws MalformedURLException {
        assertEquals(PrometheusSinkUtil.getHttpHostByURL(new URL("http://localhost:8080")), new HttpHost(null, "localhost", 8080));
    }

    @Test
    public void get_url_by_url_string_negative_test() {
        assertThrows(RuntimeException.class, () -> PrometheusSinkUtil.getURLByUrlString("ht://localhost:8080"));
    }

    @Test
    public void get_http_host_by_url_negative_test() {
        assertThrows(RuntimeException.class, () -> PrometheusSinkUtil.getHttpHostByURL(new URL("http://localhost:8080/h?s=^IXIC")));
    }

}
