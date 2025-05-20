/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.sqs;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.equalTo;

import java.util.UUID;

public class SqsSinkDlqDataTest {
    private SqsSinkDlqData sqsSinkDlqData;
    
    @BeforeEach
    void setUp() {
    }

    SqsSinkDlqData createObjectUnderTest(String message, Object data) {
        return SqsSinkDlqData.createDlqData(data, message);
    }

    @Test
    void TestBasic() {
        final String message = UUID.randomUUID().toString();
        final String data = UUID.randomUUID().toString();
        sqsSinkDlqData = createObjectUnderTest(message, data);
        assertThat(sqsSinkDlqData.getMessage(), equalTo(message));
        assertThat(sqsSinkDlqData.getData(), equalTo(data));
        assertThat(sqsSinkDlqData.hashCode(), notNullValue());
        assertTrue(sqsSinkDlqData.toString().contains("SqsSinkDlqData{"));
    }

    @Test
    void TestEquals() {
        final String message = UUID.randomUUID().toString();
        final String data = UUID.randomUUID().toString();
        sqsSinkDlqData = createObjectUnderTest(message, data);
        SqsSinkDlqData sqsSinkDlqData2 = createObjectUnderTest(message, data);
        assertTrue(sqsSinkDlqData.equals(sqsSinkDlqData2));
        assertTrue(sqsSinkDlqData.equals(sqsSinkDlqData));
        SqsSinkDlqData sqsSinkDlqData3 = createObjectUnderTest(message, data+data);
        assertFalse(sqsSinkDlqData.equals(sqsSinkDlqData3));
        Integer testInteger = 5;
        assertFalse(sqsSinkDlqData.equals(testInteger));
        assertFalse(sqsSinkDlqData.equals(null));
    }

}


