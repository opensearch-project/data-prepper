package org.opensearch.dataprepper.plugins.sink.util;

import org.apache.hc.core5.http.HttpHost;
import org.junit.jupiter.api.Test;

import java.net.MalformedURLException;
import java.net.URL;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class HttpSinkUtilTest {

    @Test
    public void get_url_by_url_string_positive_test() throws MalformedURLException {
        assertEquals(HttpSinkUtil.getURLByUrlString("http://localhost:8080"), new URL("http://localhost:8080"));
    }

    @Test
    public void get_http_host_by_url_positive_test() throws MalformedURLException {
        assertEquals(HttpSinkUtil.getHttpHostByURL(new URL("http://localhost:8080")), new HttpHost(null, "localhost", 8080));
    }

    @Test
    public void get_url_by_url_string_negative_test() {
        assertThrows(RuntimeException.class, () -> HttpSinkUtil.getURLByUrlString("ht://localhost:8080"));
    }

    @Test
    public void get_http_host_by_url_negative_test() {
        assertThrows(RuntimeException.class, () -> HttpSinkUtil.getHttpHostByURL(new URL("http://localhost:8080/h?s=^IXIC")));
    }

}
