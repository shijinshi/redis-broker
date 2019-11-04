package cn.shijinshi.redis.common.util;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Gui Jiahai
 */
public class SimpleThreadFactory implements ThreadFactory {

    private static final AtomicInteger threadNumber = new AtomicInteger(0);

    private final String name;
    private final boolean daemon;

    public SimpleThreadFactory(String name, boolean daemon) {
        this.name = name;
        this.daemon = daemon;
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread t = new Thread(r);
        if (t.isDaemon() != daemon) {
            t.setDaemon(daemon);
        }
        t.setName(name + "-" + threadNumber.incrementAndGet());
        if (t.getPriority() != Thread.NORM_PRIORITY) {
            t.setPriority(Thread.NORM_PRIORITY);
        }
        return t;
    }
}
