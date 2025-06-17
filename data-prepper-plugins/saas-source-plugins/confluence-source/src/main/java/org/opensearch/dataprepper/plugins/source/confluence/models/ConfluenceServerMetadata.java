/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */


package org.opensearch.dataprepper.plugins.source.confluence.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;

/**
 * The result of a SystemInfo API call.
 */
@Getter
@JsonIgnoreProperties(ignoreUnknown = true)
public class ConfluenceServerMetadata {

    Logger log = LoggerFactory.getLogger(ConfluenceServerMetadata.class);

    @JsonProperty("cloudId")
    private String cloudId = null;

    @JsonProperty("defaultTimeZone")
    private ZoneId defaultTimeZone = ZoneId.of("UTC");

}
