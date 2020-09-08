package com.amazon.situp.plugins.source.apmtracesource.http.common;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public final class DaemonThreadFactory implements ThreadFactory {
  final ThreadGroup group;
  final AtomicInteger threadNumber = new AtomicInteger(1);
  final String namePrefix;

  public DaemonThreadFactory(final String namePrefix) {
    this.namePrefix = namePrefix;
    SecurityManager s = System.getSecurityManager();
    group = (s != null) ? s.getThreadGroup() :
        Thread.currentThread().getThreadGroup();
  }

  @Override
  public Thread newThread(Runnable r) {
    Thread t = new Thread(group, r,
        namePrefix + "[T#" + threadNumber.getAndIncrement() + "]",
        0);
    t.setDaemon(true);
    return t;
  }
}
