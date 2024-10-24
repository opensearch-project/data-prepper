package org.opensearch.dataprepper.plugins.lambda.processor;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ResponseCardinalityTest {

    @Test
    public void testFromStringWithValidValue() {
        assertEquals(ResponseCardinality.STRICT, ResponseCardinality.fromString("strict"));
        assertEquals(ResponseCardinality.AGGREGATE, ResponseCardinality.fromString("aggregate"));
    }

    @Test
    public void testFromStringWithInvalidValue() {
        assertEquals(ResponseCardinality.STRICT, ResponseCardinality.fromString("invalid-value"));
    }

    @Test
    public void testFromStringWithNullValue() {
        assertEquals(ResponseCardinality.STRICT, ResponseCardinality.fromString(null));
    }

    @Test
    public void testGetValue() {
        assertEquals("strict", ResponseCardinality.STRICT.getValue());
        assertEquals("aggregate", ResponseCardinality.AGGREGATE.getValue());
    }
}

