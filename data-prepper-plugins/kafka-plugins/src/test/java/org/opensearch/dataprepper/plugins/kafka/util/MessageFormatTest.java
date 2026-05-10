/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(MockitoExtension.class)
class MessageFormatTest {
	@ParameterizedTest
	@EnumSource(MessageFormat.class)
	void getByNameSupportedTest(final MessageFormat name) {
		assertThat(MessageFormat.getByMessageFormatByName(name.name()), is(name));
	}

	@Test
	void getByNameUnsupportedTest() {
		assertThrows(IllegalArgumentException.class, () -> MessageFormat.getByMessageFormatByName("unknown"));
	}
}
