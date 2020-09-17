package com.amazon.situp.plugins.sink.elasticsearch;

import org.elasticsearch.common.unit.TimeValue;

import java.util.Iterator;

public final class BackOffUtils {
    private final Iterator<TimeValue> iterator;

    private long currTime = 0;

    public BackOffUtils(final Iterator<TimeValue> iterator) {
        this.iterator = iterator;
    }

    public boolean hasNext() {
        return iterator.hasNext();
    }

    public boolean next() throws InterruptedException {
        if (!iterator.hasNext()) {
            return false;
        } else {
            final long nextTime = iterator.next().getMillis();
            Thread.sleep(nextTime - currTime);
            currTime = nextTime;
            return true;
        }
    }
}
