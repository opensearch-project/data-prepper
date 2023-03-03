/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.util;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public enum MessageFormat {
	STRING("string"), JSON("json"), AVRO("avro");

	private static final Map<String, MessageFormat> MESSAGE_FORMAT_MAP = Arrays.stream(MessageFormat.values())
			.collect(Collectors.toMap(MessageFormat::toString, Function.identity()));

	private final String name;

	MessageFormat(final String name) {
		this.name = name;
	}

	@Override
	public String toString() {
		return this.name;
	}

	public static MessageFormat getByName(final String name) {
		return MESSAGE_FORMAT_MAP.get(name.toLowerCase());
	}
}
