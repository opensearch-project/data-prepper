package org.opensearch.dataprepper.plugins.source.saas.crawler.base;


import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public abstract class ItemInfo {
    String id;

    public abstract String getKeyAttributes();
}
