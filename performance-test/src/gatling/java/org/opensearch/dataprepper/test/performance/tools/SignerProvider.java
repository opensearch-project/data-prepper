package org.opensearch.dataprepper.test.performance.tools;

import io.gatling.http.client.Request;

import java.util.function.Consumer;

public class SignerProvider {
    private static final Consumer<Request> NO_OP_SIGNER = r -> { };

    public static Consumer<Request> getSigner() {
        String authentication = System.getProperty("authentication");

        if(AwsRequestSigner.SIGNER_NAME.equals(authentication)) {
            return new AwsRequestSigner();
        }

        return NO_OP_SIGNER;
    }
}
