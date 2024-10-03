package org.opensearch.dataprepper.plugins.source.saas.jira;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.opensearch.dataprepper.plugins.source.saas.crawler.base.ItemInfo;

@Setter
@Getter
public class JiraItemInfo extends ItemInfo {
    private String project;
    private String issueType;

    public JiraItemInfo(String id, String project, String issueType) {
        super(id);
        this.project = project;
        this.issueType = issueType;
    }

    public String getKeyAttributes() {
        return project+"|"+issueType;
    }

}
