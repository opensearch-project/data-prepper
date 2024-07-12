/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.obfuscation.action;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.processor.obfuscation.ObfuscationProcessor;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;


public class OneWayHashActionTest {

    private OneWayHashAction oneWayHashAction;

    @Test
    void testObfuscateWithPatternAsNull() {
        String message = "Hello";
        OneWayHashActionConfig config = new OneWayHashActionConfig("SHA-512","");
        String result = new OneWayHashAction(config).obfuscate(message, null);
        assertNotNull(result);
        assertThat(result.length(), equalTo(88));        
    }

    private OneWayHashAction createOneWayHashAction(String salt) {
        final OneWayHashActionConfig config = new OneWayHashActionConfig(salt);
        return new OneWayHashAction(config); 
    }

 
    @ParameterizedTest
    @CsvSource({
            "Hello,AAAAAAAAAAAAAAAA,2NYZBaQ9nySumhHENpiKatKJhU3jqHC8jJ4DZC612RPGvkzPK1K12DskOI8Cn3qeOMSCTNIWErcGZr8JV4i9HQ==",
            "Hi,BBBBBBBBBBBBBBBB,s3S4lyurJvJpQJ6EHN3gi/kexv79Ox+nIqXuVdbvgZP0b718AAxX0bOCPLeOZCnq3p3+DS+a0q0xLSJoMqjsNQ==",
            "Hello,CCCCCCCCCCCCCCCC,SsUUpl/+GtU7cRg3ffuRKAtPU7cftdN440sNKR+gABy6JV6crwn5VTNSIqGKaTgBcZeYICy2ZmxP1DiHcW31rA==",
            "H,DDDDDDDDDDDDDDDD,XR6utNkOp9te4+0vaRE0+ky/Zyw/gok1sI8qR/stZqFPoU733KwFcur36FCTUZd+i/UpyyJ9L/W6ObwPIf7iuw==",
                                
    })
    void testObfuscateWithDifferentConfig(String message, String salt, String expected) {
        oneWayHashAction = createOneWayHashAction(salt);
        List<Pattern> patterns = new ArrayList<>();
        String result = oneWayHashAction.obfuscate(message, patterns);
        assertThat(result, equalTo(expected));
    }


    @ParameterizedTest
    @CsvSource({            
            "testing this functionality, test, AAAAAAAAAAAAAAAA, ILsULwmg32tiEQGqeX1rpWI9PGZXSX2Q9tRzXCD0cD/OKMMEBEXKYZhnXj1Xr9q+Dxa11iOmuXd+hx4ZTUaBCg==ing this functionality",
            "test this functionality, test, BBBBBBBBBBBBBBBB, QT4wuvJSvgrxa/27gf4cZ1jzeNyiOnDxsY0oS7SsC/eVpBNyhj2I8Rh6/wCsvqRyzAvVoksTKOuRzSFUm6vAQw== this functionality",
            "another test of this functionality, test, CCCCCCCCCCCCCCCC, another H9YrqOIlLtaoSCkNR2M0go3npf118KbsHFemyvJUX4+zt8FvjoiReq/0pk5va5i+7eX6XTOMwNokUUl4r+PTHw== of this functionality",
            "My name is Bob and my email address is abc@example.com as of now and xyz@example.org in the future, [A-Za-z0-9+_.-]+@([\\w-]+\\.)+[\\w-] ,DDDDDDDDDDDDDDDD, My name is Bob and my email address is DdijIn6L3Cs4+PCYwCy+3bzLZ7w228quoodeI+VDlyMeFe+uZ/Ec1x/DK7MHSmZm8N5SZrINhvGgyig7aEBflg==om as of now and XQGlFjysVX1lkTFoRVCY+QEOfOf6nCoaRy5lxGAHyaFRgMGDpq93PwgZd18DZ3ZfWFRCwgPDGaExJDuRa0kkEQ==rg in the future",
    })
    void testObfuscateWithPatterns(String message, String pattern, String salt, String expected) {        

        oneWayHashAction = createOneWayHashAction(salt);

        Pattern compiledPattern = Pattern.compile(pattern);
        List<Pattern> patterns = new ArrayList<>();
        patterns.add(compiledPattern);
        String result = oneWayHashAction.obfuscate(message, patterns);        
        assertThat(result, equalTo(expected));
    }

}
