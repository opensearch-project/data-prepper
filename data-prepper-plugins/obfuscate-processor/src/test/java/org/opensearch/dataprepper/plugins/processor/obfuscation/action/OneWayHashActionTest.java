/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.obfuscation.action;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.opensearch.dataprepper.model.event.EventKeyFactory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.opensearch.dataprepper.model.pattern.Pattern;

import org.opensearch.dataprepper.event.TestEventKeyFactory;
import org.opensearch.dataprepper.model.event.EventKeyFactory;

import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class OneWayHashActionTest implements ObfuscationActionTest {

    @Mock
    OneWayHashActionConfig mockConfig;

    private final EventKeyFactory eventKeyFactory = TestEventKeyFactory.getTestEventFactory();

    @Test
    void testObfuscateWithPatternAsNull() {
        String message = "Hello";    
        when(mockConfig.getSaltKey()).thenReturn(null);
        when(mockConfig.getSalt()).thenReturn("");
        when(mockConfig.getFormat()).thenReturn("SHA-512");

        String result = new OneWayHashAction(mockConfig).obfuscate(message, null,createRecord(message));
        assertNotNull(result);
        
        assertThat(result, not(containsString(message)));
        assertThat(result.length(), equalTo(88));        
    }

 
    @ParameterizedTest    
    @CsvSource({
            "Hello,AAAAAAAAAAAAAAAA,2NYZBaQ9nySumhHENpiKatKJhU3jqHC8jJ4DZC612RPGvkzPK1K12DskOI8Cn3qeOMSCTNIWErcGZr8JV4i9HQ==",
            "Hi,BBBBBBBBBBBBBBBB,s3S4lyurJvJpQJ6EHN3gi/kexv79Ox+nIqXuVdbvgZP0b718AAxX0bOCPLeOZCnq3p3+DS+a0q0xLSJoMqjsNQ==",
            "Hello,CCCCCCCCCCCCCCCC,SsUUpl/+GtU7cRg3ffuRKAtPU7cftdN440sNKR+gABy6JV6crwn5VTNSIqGKaTgBcZeYICy2ZmxP1DiHcW31rA==",
            "H,DDDDDDDDDDDDDDDD,XR6utNkOp9te4+0vaRE0+ky/Zyw/gok1sI8qR/stZqFPoU733KwFcur36FCTUZd+i/UpyyJ9L/W6ObwPIf7iuw==",                                
    })
    void testObfuscateWithDifferentConfig(String message, String salt, String expected) {        

        when(mockConfig.getSalt()).thenReturn(salt);
        when(mockConfig.getSaltKey()).thenReturn(null);
        when(mockConfig.getFormat()).thenReturn("SHA-512");

        OneWayHashAction oneWayHashAction = new OneWayHashAction(mockConfig);

        List<Pattern> patterns = new ArrayList<>();
        String result = oneWayHashAction.obfuscate(message, patterns,createRecord(message));
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

        when(mockConfig.getSalt()).thenReturn(salt);
        when(mockConfig.getFormat()).thenReturn("SHA-512");

        OneWayHashAction oneWayHashAction = new OneWayHashAction(mockConfig);
        

        Pattern compiledPattern = Pattern.compile(pattern);
        List<Pattern> patterns = new ArrayList<>();
        patterns.add(compiledPattern);
        String result = oneWayHashAction.obfuscate(message, patterns,createRecord(message));        
        assertThat(result, equalTo(expected));
    }

    @ParameterizedTest
    @CsvSource({            
            "testing this functionality and this test, test, this, AAAAAAAAAAAAAAAA, ILsULwmg32tiEQGqeX1rpWI9PGZXSX2Q9tRzXCD0cD/OKMMEBEXKYZhnXj1Xr9q+Dxa11iOmuXd+hx4ZTUaBCg==ing VsljIdInUvEk2ShjqBF94jgwWDk1lqcE/Fmb/LACPRlwIKsdmlk2PPX2o0XHObp4kRDqd+gUU5iUa/4HXhaA8g== functionality and VsljIdInUvEk2ShjqBF94jgwWDk1lqcE/Fmb/LACPRlwIKsdmlk2PPX2o0XHObp4kRDqd+gUU5iUa/4HXhaA8g== ILsULwmg32tiEQGqeX1rpWI9PGZXSX2Q9tRzXCD0cD/OKMMEBEXKYZhnXj1Xr9q+Dxa11iOmuXd+hx4ZTUaBCg==",
            "test this functionality, test, this, BBBBBBBBBBBBBBBB, QT4wuvJSvgrxa/27gf4cZ1jzeNyiOnDxsY0oS7SsC/eVpBNyhj2I8Rh6/wCsvqRyzAvVoksTKOuRzSFUm6vAQw== LAD8UPdf/1cMoKY7Py17uRFNA+OEpVpa9lulTW8wEhsfQsDf/FvBIYxt/YO04sBI8CA1WY+i4elM5nY0xh13Lw== functionality",
            "another test of this functionality, test, this, CCCCCCCCCCCCCCCC, another H9YrqOIlLtaoSCkNR2M0go3npf118KbsHFemyvJUX4+zt8FvjoiReq/0pk5va5i+7eX6XTOMwNokUUl4r+PTHw== of oAY9W4VW35Z14mrUisMks9mTILHsswbjjrJt96swt20/lnkMyf0izXV8OhQIh2N7Ml88uXU1fUfk0jTq41udfw== functionality",
            "My name is Bob and my email address is abc@example.com as of now and xyz@example.org in the future, [A-Za-z0-9+_.-]+@([\\w-]+\\.)+[\\w-], Bob ,DDDDDDDDDDDDDDDD, My name is aDNCnlEqYbJO9KKnHEhhJSSyy2BB10CUSJxRMCSGLD1gdRNFVTo+Pz7xFepWfVOhuUGulvbnitdPoc8JIlEIFg== and my email address is DdijIn6L3Cs4+PCYwCy+3bzLZ7w228quoodeI+VDlyMeFe+uZ/Ec1x/DK7MHSmZm8N5SZrINhvGgyig7aEBflg==om as of now and XQGlFjysVX1lkTFoRVCY+QEOfOf6nCoaRy5lxGAHyaFRgMGDpq93PwgZd18DZ3ZfWFRCwgPDGaExJDuRa0kkEQ==rg in the future",
    })
    void testObfuscateWithTwoPatterns(String message, String pattern1, String pattern2, String salt, String expected) {        

        when(mockConfig.getSalt()).thenReturn(salt);
        when(mockConfig.getFormat()).thenReturn("SHA-512");

        OneWayHashAction oneWayHashAction = new OneWayHashAction(mockConfig);

        Pattern compiledPattern1 = Pattern.compile(pattern1);
        Pattern compiledPattern2 = Pattern.compile(pattern2);

        List<Pattern> patterns = new ArrayList<>();
        patterns.add(compiledPattern1);
        patterns.add(compiledPattern2);
        String result = oneWayHashAction.obfuscate(message, patterns,createRecord(message));        
        assertThat(result, equalTo(expected));
    }

    @ParameterizedTest
    @CsvSource({            
            "testing this functionality, test, AAAAAAAAAAAAAAAA, ILsULwmg32tiEQGqeX1rpWI9PGZXSX2Q9tRzXCD0cD/OKMMEBEXKYZhnXj1Xr9q+Dxa11iOmuXd+hx4ZTUaBCg==ing this functionality",
            "test this functionality, test, BBBBBBBBBBBBBBBB, QT4wuvJSvgrxa/27gf4cZ1jzeNyiOnDxsY0oS7SsC/eVpBNyhj2I8Rh6/wCsvqRyzAvVoksTKOuRzSFUm6vAQw== this functionality",
            "another test of this functionality, test, CCCCCCCCCCCCCCCC, another H9YrqOIlLtaoSCkNR2M0go3npf118KbsHFemyvJUX4+zt8FvjoiReq/0pk5va5i+7eX6XTOMwNokUUl4r+PTHw== of this functionality",
            "My name is Bob and my email address is abc@example.com as of now and xyz@example.org in the future, [A-Za-z0-9+_.-]+@([\\w-]+\\.)+[\\w-] ,DDDDDDDDDDDDDDDD, My name is Bob and my email address is DdijIn6L3Cs4+PCYwCy+3bzLZ7w228quoodeI+VDlyMeFe+uZ/Ec1x/DK7MHSmZm8N5SZrINhvGgyig7aEBflg==om as of now and XQGlFjysVX1lkTFoRVCY+QEOfOf6nCoaRy5lxGAHyaFRgMGDpq93PwgZd18DZ3ZfWFRCwgPDGaExJDuRa0kkEQ==rg in the future",
    })
    void testObfuscateWithPatternsAndInvalidSaltKey(String message, String pattern, String salt, String expected) {        

        //adding SaltKey that cannot be found, to ensure that logic is defaulted back to the configured salt value.    
        when(mockConfig.getSaltKey()).thenReturn(eventKeyFactory.createEventKey("id"));
        when(mockConfig.getSalt()).thenReturn(salt);
        when(mockConfig.getFormat()).thenReturn("SHA-512");

        OneWayHashAction oneWayHashAction = new OneWayHashAction(mockConfig); 

        Pattern compiledPattern = Pattern.compile(pattern);
        List<Pattern> patterns = new ArrayList<>();
        patterns.add(compiledPattern);
        String result = oneWayHashAction.obfuscate(message, patterns,createRecord(message));        
        assertThat(result, equalTo(expected));
    }

    @ParameterizedTest
    @CsvSource({            
            "testing this functionality, test, AAAAAAAAAAAAAAAA, 8g+p3Td+ClA+PttgNrZ8Qsg+tIc9/46TwNDtLeM6lQILI8jcQzPz0bOUM4IrbTlqgHYuOD8r6j6EElj4E6dZLw==ing this functionality",
            "test this functionality, test, BBBBBBBBBBBBBBBB, 8g+p3Td+ClA+PttgNrZ8Qsg+tIc9/46TwNDtLeM6lQILI8jcQzPz0bOUM4IrbTlqgHYuOD8r6j6EElj4E6dZLw== this functionality",
            "another test of this functionality, test, CCCCCCCCCCCCCCCC, another 8g+p3Td+ClA+PttgNrZ8Qsg+tIc9/46TwNDtLeM6lQILI8jcQzPz0bOUM4IrbTlqgHYuOD8r6j6EElj4E6dZLw== of this functionality",
            "My name is Bob and my email address is abc@example.com as of now and xyz@example.org in the future, [A-Za-z0-9+_.-]+@([\\w-]+\\.)+[\\w-] ,DDDDDDDDDDDDDDDD, My name is Bob and my email address is 9zuqdjZfSkx7Xh6rO7bxRpREOmEA8EdtlNXOSviW6C41+sAK2QE/z9PGtRTf+T4bvTuzWBVv7SKVov6jII5+gw==om as of now and KAn0LtIRQYzoPtJqHczu21+gWcXl1OUUwbT9nY+2s+6164/PG4OuW/CZJIUZvOfrUICiL6BUJE32JCEaOfrwjA==rg in the future",
    })
    void testObfuscateWithPatternsAndValidSaltKey(String message, String pattern, String salt, String expected) {        

        //adding SaltKey that cannot be found, to ensure that logic is defaulted back to the configured salt value.        
        when(mockConfig.getSaltKey()).thenReturn(eventKeyFactory.createEventKey("message"));
        when(mockConfig.getSalt()).thenReturn(salt);
        when(mockConfig.getFormat()).thenReturn("SHA-512");

        OneWayHashAction oneWayHashAction = new OneWayHashAction(mockConfig); 

        final Map<String, Object> testData = new HashMap<>();
        testData.put("message", message);        
        Pattern compiledPattern = Pattern.compile(pattern);
        List<Pattern> patterns = new ArrayList<>();
        patterns.add(compiledPattern);

        String result = oneWayHashAction.obfuscate(message, patterns,createRecord("12345"));                
        assertThat(result, equalTo(expected));
    }

}
