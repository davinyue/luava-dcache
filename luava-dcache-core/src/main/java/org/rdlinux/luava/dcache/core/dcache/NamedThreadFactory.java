package org.rdlinux.luava.dcache.core.dcache;

import reactor.util.annotation.NonNull;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class NamedThreadFactory implements ThreadFactory {
    private final ThreadGroup threadGroup;

    private final AtomicInteger threadNumber = new AtomicInteger(1);
    private final String name;

    public NamedThreadFactory(String name) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name can not be empty");
        }
        this.name = name;
        this.threadGroup = new ThreadGroup(name);
        this.threadGroup.setDaemon(true);
    }

    @Override
    public Thread newThread(@NonNull Runnable ra) {
        Thread t = new Thread(this.threadGroup, ra, this.name + "-" + this.threadNumber.getAndIncrement());
        t.setDaemon(true);
        return t;
    }
}
