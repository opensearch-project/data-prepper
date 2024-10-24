package org.opensearch.dataprepper.plugins.lambda.processor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.lambda.common.config.InvocationType;

public class InvocationTypeTest {

    @Test
    public void testFromStringWithValidValue() {
        assertEquals(InvocationType.REQUEST_RESPONSE, InvocationType.fromString("request-response"));
        assertEquals(InvocationType.EVENT, InvocationType.fromString("event"));
    }

    @Test
    public void testFromStringWithNullValue() {
        assertEquals(InvocationType.REQUEST_RESPONSE, InvocationType.fromStringDefaultsToRequestResponse(null));
        assertEquals(InvocationType.EVENT, InvocationType.fromStringDefaultsToEvent(null));
    }

    @Test
    public void testGetValue() {
        assertEquals("request-response", InvocationType.REQUEST_RESPONSE.getUserInputValue());
        assertEquals("event", InvocationType.EVENT.getUserInputValue());
        assertEquals("RequestResponse", InvocationType.REQUEST_RESPONSE.getAwsLambdaValue());
        assertEquals("Event", InvocationType.EVENT.getAwsLambdaValue());
    }
}

