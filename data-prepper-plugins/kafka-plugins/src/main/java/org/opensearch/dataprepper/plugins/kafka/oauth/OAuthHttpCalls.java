/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.oauth;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

public class OAuthHttpCalls {

    private static final Logger LOG = LoggerFactory.getLogger(OAuthHttpCalls.class);

    private static String OAUTH_LOGIN_SERVER;
    private static String OAUTH_LOGIN_ENDPOINT;
    private static String OAUTH_LOGIN_GRANT_TYPE;
    private static String OAUTH_LOGIN_SCOPE;

    private static String OAUTH_INTROSPECT_SERVER;
    private static String OAUTH_INTROSPECT_ENDPOINT;

    private static String OAUTH_LOGIN_AUTHORIZATION;
    private static String OAUTH_INTROSPECT_AUTHORIZATION;

    private static boolean OAUTH_ACCEPT_UNSECURE_SERVER;
    private static boolean OAUTH_WITH_SSL;
    private static Time time = Time.SYSTEM;

    public static void acceptUnsecureServer() {
        if (OAUTH_ACCEPT_UNSECURE_SERVER) {
            TrustManager[] trustAllCerts = new TrustManager[]{
                    new X509TrustManager() {
                        public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                            return null;
                        }

                        public void checkClientTrusted(
                                java.security.cert.X509Certificate[] certs, String authType) {
                        }

                        public void checkServerTrusted(
                                java.security.cert.X509Certificate[] certs, String authType) {
                        }
                    }
            };
            try {
                SSLContext sc = SSLContext.getInstance("SSL");
                sc.init(null, trustAllCerts, new java.security.SecureRandom());
                HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
            } catch (NoSuchAlgorithmException | KeyManagementException e) {
                LOG.error("Error while getting secure SSL server details :", e);
            }
        }
    }

    public static OAuthBearerTokenJwt login(Map<String, String> options) {
        OAuthBearerTokenJwt result = null;
        try {
            setPropertyValues(options);
            acceptUnsecureServer();
            long callTime = time.milliseconds();

            //Mount POST data
            String grantType = "grant_type=" + OAUTH_LOGIN_GRANT_TYPE;
            String scope = "scope=" + OAUTH_LOGIN_SCOPE;
            String postDataStr = grantType + "&" + scope;

            LOG.info("Trying to get oauth login details...");
            LOG.info("OAuth Login Server URL:" + OAUTH_LOGIN_SERVER);
            LOG.info("OAuth Login EndPoint:" + OAUTH_LOGIN_ENDPOINT);
            LOG.info("OAuth Authorization Token:" + OAUTH_LOGIN_AUTHORIZATION);

            Map<String, Object> resp = null;
            if (OAUTH_WITH_SSL) {
                resp = doHttpsCall(OAUTH_LOGIN_SERVER + OAUTH_LOGIN_ENDPOINT, postDataStr, OAUTH_LOGIN_AUTHORIZATION);
            } else {
                resp = doHttpCall(OAUTH_LOGIN_SERVER + OAUTH_LOGIN_ENDPOINT, postDataStr, OAUTH_LOGIN_AUTHORIZATION);
            }

            if (resp != null) {
                String accessToken = (String) resp.get("access_token");
                long expiresIn = ((Integer) resp.get("expires_in")).longValue();
                String clientId = (String) resp.get("client_id");
                result = new OAuthBearerTokenJwt(accessToken, expiresIn, callTime, clientId);
            } else {
                throw new Exception("Null or Empty response while login...");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    private static void setPropertyValues(Map<String, String> options) {
        OAUTH_LOGIN_SERVER = (String) getPropertyValue(options, "OAUTH_LOGIN_SERVER", "");
        OAUTH_LOGIN_ENDPOINT = (String) getPropertyValue(options, "OAUTH_LOGIN_ENDPOINT", "");
        OAUTH_LOGIN_GRANT_TYPE = (String) getPropertyValue(options, "OAUTH_LOGIN_GRANT_TYPE", "");
        OAUTH_LOGIN_SCOPE = (String) getPropertyValue(options, "OAUTH_LOGIN_SCOPE", "");

        OAUTH_INTROSPECT_SERVER = (String) getPropertyValue(options, "OAUTH_INTROSPECT_SERVER", "");
        OAUTH_INTROSPECT_ENDPOINT = (String) getPropertyValue(options, "OAUTH_INTROSPECT_ENDPOINT", "");

        OAUTH_LOGIN_AUTHORIZATION = (String) getPropertyValue(options, "OAUTH_AUTHORIZATION", "");
        OAUTH_INTROSPECT_AUTHORIZATION = (String) getPropertyValue(options, "OAUTH_INTROSPECT_AUTHORIZATION", "");

        OAUTH_ACCEPT_UNSECURE_SERVER = (Boolean) getPropertyValue(options, "OAUTH_ACCEPT_UNSECURE_SERVER", false);
        OAUTH_WITH_SSL = (Boolean) getPropertyValue(options, "OAUTH_WITH_SSL", true);
    }

    public static OAuthBearerTokenJwt introspectBearer(Map<String, String> options, String accessToken) {
        OAuthBearerTokenJwt result = null;
        try {
            setPropertyValues(options);
            //Mount POST data
            String token = "token=" + accessToken;

            LOG.info("Try to get introspect oauth details...");
            LOG.info("Oauth Introspect Server URL:" + OAUTH_INTROSPECT_SERVER);
            LOG.info("Oauth Introspect EndPoint URL:" + OAUTH_INTROSPECT_ENDPOINT);
            LOG.info("Oauth Authorization Token:" + OAUTH_INTROSPECT_AUTHORIZATION);

            Map<String, Object> resp = null;
            if (OAUTH_WITH_SSL) {
                resp = doHttpsCall(OAUTH_INTROSPECT_SERVER + OAUTH_INTROSPECT_ENDPOINT, token, OAUTH_INTROSPECT_AUTHORIZATION);
            } else {
                resp = doHttpCall(OAUTH_INTROSPECT_SERVER + OAUTH_INTROSPECT_ENDPOINT, token, OAUTH_INTROSPECT_AUTHORIZATION);
            }
            if (resp != null) {
                if ((boolean) resp.get("active")) {
                    result = new OAuthBearerTokenJwt(resp, accessToken);
                } else {
                    throw new Exception("Token has been expired...!");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    private static Map<String, Object> doHttpCall(String urlStr, String postParameters, String oauthToken) {
        try {
            LOG.info("doHttpCall ->");
            acceptUnsecureServer();

            byte[] postData = postParameters.getBytes(StandardCharsets.UTF_8);
            int postDataLength = postData.length;

            URL url = new URL("http://" + urlStr);
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setInstanceFollowRedirects(true);
            con.setRequestMethod("POST");
            con.setRequestProperty("Authorization", oauthToken);
            con.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
            con.setRequestProperty("charset", "utf-8");
            con.setRequestProperty("Content-Length", Integer.toString(postDataLength));
            con.setUseCaches(false);
            con.setDoOutput(true);

            try (DataOutputStream wr = new DataOutputStream(con.getOutputStream())) {
                wr.write(postData);
            }

            int responseCode = con.getResponseCode();
            if (responseCode == 200) {
                return handleJsonResponse(con.getInputStream());
            } else {
                throw new Exception("Return code " + responseCode);
            }
        } catch (Exception e) {
            LOG.error("at doHttpCall", e);
        }
        return null;
    }

    private static Map<String, Object> doHttpsCall(String urlStr, String postParameters, String oauthToken) {
        try {
            acceptUnsecureServer();

            byte[] postData = postParameters.getBytes(StandardCharsets.UTF_8);
            int postDataLength = postData.length;

            URL url = new URL("https://" + urlStr);
            HttpsURLConnection con = (HttpsURLConnection) url.openConnection();
            con.setInstanceFollowRedirects(true);
            con.setRequestMethod("POST");
            con.setRequestProperty("Authorization", oauthToken);
            con.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
            con.setRequestProperty("charset", "utf-8");
            con.setRequestProperty("Content-Length", Integer.toString(postDataLength));
            con.setUseCaches(false);
            con.setDoOutput(true);

            try (DataOutputStream wr = new DataOutputStream(con.getOutputStream())) {
                wr.write(postData);
            }

            int responseCode = con.getResponseCode();
            if (responseCode == 200) {
                return handleJsonResponse(con.getInputStream());
            } else {
                throw new Exception("Return code " + responseCode);
            }
        } catch (Exception e) {
            LOG.error("at doHttpCall", e);
        }
        return null;
    }

    private static Object getPropertyValue(Map<String, String> options, String propertyName, Object defaultValue) {
        Object result = null;
        String env = options.get(propertyName) != null ? options.get(propertyName) : System.getProperty(propertyName);
        if ("OAUTH_AUTHORIZATION".equals(propertyName) || "OAUTH_INTROSPECT_AUTHORIZATION".equals(propertyName)) {
            env = env.replace("%20", " ");
        }
        if (env == null) {
            result = defaultValue;
        } else {
            if (defaultValue instanceof Boolean) {
                result = Boolean.valueOf(env);
            } else if (defaultValue instanceof Integer) {
                result = Integer.valueOf(env);
            } else if (defaultValue instanceof Double) {
                result = Double.valueOf(env);
            } else if (defaultValue instanceof Float) {
                result = Float.valueOf(env);
            } else {
                result = env;
            }
        }
        return result;
    }

    private static Map<String, Object> handleJsonResponse(InputStream inputStream) {
        Map<String, Object> result = null;
        try {
            BufferedReader in = new BufferedReader(new InputStreamReader(inputStream));
            String inputLine;
            StringBuffer response = new StringBuffer();

            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();
            String jsonResponse = response.toString();
            ObjectMapper objectMapper = new ObjectMapper();
            result = objectMapper.readValue(jsonResponse, new TypeReference<Map<String, Object>>() {
            });

        } catch (Exception e) {
            LOG.error("Error while getting the response from the server", e);
        }
        return result;
    }
}
