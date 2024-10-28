package org.opensearch.dataprepper.plugins.source.jira.utils;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public enum JiraContentType {
    PROJECT("PROJECT"),
    ISSUE("ISSUE"),
    COMMENT("COMMENT"),
    ATTACHMENT("ATTACHMENT"),
    WORKLOG("WORKLOG");

    @Getter
    private final String type;
}
