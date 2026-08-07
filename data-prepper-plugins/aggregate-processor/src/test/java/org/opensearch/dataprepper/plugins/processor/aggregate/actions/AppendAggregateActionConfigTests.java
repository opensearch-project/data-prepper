/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.processor.aggregate.actions;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.opensearch.dataprepper.test.helper.ReflectivelySetField.setField;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class AppendAggregateActionConfigTests {
	private AppendAggregateActionConfig agentAggregateActionConfig;

	private AppendAggregateActionConfig createObjectUnderTest() {
		return new AppendAggregateActionConfig();
	}

	@BeforeEach
	void setup() {
		agentAggregateActionConfig = createObjectUnderTest();
	}

	@Test
	void testValidConfig() throws NoSuchFieldException, IllegalAccessException {
		final List<String> testKeysToAppend = new ArrayList<>();
		testKeysToAppend.add(UUID.randomUUID().toString());

		setField(AppendAggregateActionConfig.class, agentAggregateActionConfig, "keysToAppend", testKeysToAppend);
		assertThat(agentAggregateActionConfig.getKeysToAppend(), equalTo(testKeysToAppend));
	}

}
