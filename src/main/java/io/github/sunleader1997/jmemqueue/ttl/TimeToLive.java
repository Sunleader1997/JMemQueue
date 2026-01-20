package io.github.sunleader1997.jmemqueue.ttl;

import java.util.concurrent.TimeUnit;

public class TimeToLive {
    private final long timeAlive;
    private final TimeUnit timeUnit;

    public TimeToLive(long timeAlive, TimeUnit timeUnit) {
        this.timeAlive = timeAlive;
        this.timeUnit = timeUnit;
    }

    public long getCleanBefore() {
        return System.currentTimeMillis() - timeUnit.toMillis(timeAlive);
    }
}
