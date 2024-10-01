package org.opensearch.dataprepper.plugins.source.neptune.client;

import com.amazonaws.neptune.auth.NeptuneApacheHttpSigV4Signer;
import com.amazonaws.neptune.auth.NeptuneSigV4SignerException;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.protocol.HttpContext;
import org.apache.http.ssl.SSLContexts;
import org.opensearch.dataprepper.plugins.source.neptune.configuration.NeptuneSourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;

// TODO: Replace with Stream SDK if it works
// https://jira.rds.a2z.com/browse/GRAPHDB-9373
public class NeptuneConnection {
    private static final Logger LOG = LoggerFactory.getLogger(NeptuneConnection.class);
    private static final String NEPTUNE_STREAM_ENDPOINT = "/stream";

    public static HttpClient getHttpClient(final NeptuneSourceConfig sourceConfig) {

       if (sourceConfig.isIamAuth()) {
           LOG.info("IAM Auth is enabled, sign requests using aws_sigv4 ...");
           try {
               return buildSigV4HttpClient(sourceConfig.getRegion());
           } catch (NeptuneSigV4SignerException e) {
               throw new RuntimeException("Problem signing the request: ", e);
           }
       } else {
           try {
               // TODO: Properly fix error "Certificate for <localhost> doesn't match any of the subject alternative names ..."
               SSLConnectionSocketFactory scsf = new SSLConnectionSocketFactory(
                       SSLContexts.custom().loadTrustMaterial(null, new TrustSelfSignedStrategy()).build(),
                       NoopHostnameVerifier.INSTANCE);
               return HttpClientBuilder.create().setSSLSocketFactory(scsf).build(); // auth not initialized, no signing performed
           } catch (Exception e) {
               throw new RuntimeException("Problem building SSL connection socket factory", e);
           }
       }

    }

    private static HttpClient buildSigV4HttpClient(final String regionName) throws NeptuneSigV4SignerException {

        final AwsCredentialsProvider awsCredentialsProvider = DefaultCredentialsProvider.create();
        final NeptuneApacheHttpSigV4Signer v4Signer = new NeptuneApacheHttpSigV4Signer(regionName, awsCredentialsProvider);

        final HttpClient v4SigningClient = HttpClientBuilder.create().addInterceptorLast(new HttpRequestInterceptor() {

            @Override
            public void process(final HttpRequest req, final HttpContext ctx) throws HttpException {
                if (req instanceof HttpUriRequest) {
                    final HttpUriRequest httpUriReq = (HttpUriRequest) req;
                    try {
                        v4Signer.signRequest(httpUriReq);
                    } catch (NeptuneSigV4SignerException e) {
                        throw new HttpException("Problem signing the request: ", e);
                    }
                } else {
                    throw new HttpException("Not an HttpUriRequest"); // this should never happen
                }
            }

        }).build();

        return v4SigningClient;
    }
}
