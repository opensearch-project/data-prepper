/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.obfuscation.action;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.List;
import org.opensearch.dataprepper.model.pattern.Pattern;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

class MaskActionTest implements ObfuscationActionTest {

    private MaskAction maskAction;


    @Test
    void testObfuscateWithPatternAsNull() {
        String message = "Hello";
        maskAction = createMaskAction("*", 3);
        String result = maskAction.obfuscate(message, null, createRecord(message));
        assertThat(result, equalTo("***"));
    }

    @ParameterizedTest
    @CsvSource({
            "Hello,*,3,***",
            "Hello,#,3,###",
            "Hello,*,6,******",
            "H,*,3,***",
    })
    void testObfuscateWithDifferentConfig(String message, String maskCharacter, int maskCharacterLength, String expected) {
        maskAction = createMaskAction(maskCharacter, maskCharacterLength);
        List<Pattern> patterns = new ArrayList<>();
        String result = maskAction.obfuscate(message, patterns,createRecord(message));
        assertThat(result, equalTo(expected));
    }

    private MaskAction createMaskAction(String maskCharacter, int maskCharacterLength) {
        final MaskActionConfig config = new MaskActionConfig(maskCharacter, maskCharacterLength);
        return new MaskAction(config);
    }
}