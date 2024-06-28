package org.opensearch.dataprepper.plugins.lambda.processor;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;

@Builder(setterPrefix = "with")
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class LambdaResult {

    private InvokeResponse lambdaResponse;

    private Boolean isUploadedToLambda;
}
