package org.opensearch.dataprepper.validations;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;

public class GrokPatternValidatorTest {

    @Test
    void validatePatterns_with_invalid_match_patterns_returns_expected_messages() {
        final Map<String, String> additionalPatterns = Map.of(
                "PATTERN1", "one_%{WORD}"
        );

        final List<String> matchPatterns = List.of("some_pattern", "%{PATTERN1}", "%{WORD} %{PATTERN10}", "%{ANOTHER_PATTERN}");
        final List<String> validationErrors = GrokPatternValidator.validatePatterns(additionalPatterns, matchPatterns);

        assertThat(validationErrors, notNullValue());
        assertThat(validationErrors.size(), equalTo(2));
        assertThat(validationErrors.get(0), startsWith("The grok match pattern \"%{WORD} %{PATTERN10}\" is invalid"));
        assertThat(validationErrors.get(1), startsWith("The grok match pattern \"%{ANOTHER_PATTERN}\" is invalid"));
    }

    @Test
    void validatePatterns_with_no_invalid_patterns_returns_empty_list() {
        final Map<String, String> additionalPatterns = Map.of(
                "PATTERN1", "one_%{WORD}",
                "PATTERN2", "test_%{NUMBER}"
        );

        final List<String> matchPatterns = List.of("some_pattern", "%{PATTERN1}", "%{WORD} %{PATTERN2}");

        final List<String> errorMessages = GrokPatternValidator.validatePatterns(additionalPatterns, matchPatterns);
        assertThat(errorMessages, notNullValue());
        assertThat(errorMessages.size(), equalTo(0));
    }

    @Test
    void validatePatterns_with_invalid_additional_patterns_returns_expected_messages() {
        final Map<String, String> additionalPatterns = new HashMap<>();
        additionalPatterns.put("test", null);

        final List<String> matchPatterns = List.of("some_pattern", "%{PATTERN1}", "%{WORD} %{PATTERN2}");

        final List<String> errorMessages = GrokPatternValidator.validatePatterns(additionalPatterns, matchPatterns);
        assertThat(errorMessages, notNullValue());
        assertThat(errorMessages.size(), equalTo(1));
        assertThat(errorMessages.get(0), startsWith("The grok pattern with name \"test\" and pattern \"null\""));
    }
}
