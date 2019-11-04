package cn.shijinshi.redis.common;

import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * 用于优雅停机
 *
 * @author Gui Jiahai
 */
@Component
public class Shutdown extends Thread {

    private final static Set<Thread> waitingThreads = new HashSet<>();
    private final static Set<Runnable> waitingRunnables = new HashSet<>();

    private List<Delayed> delayedList;

    public Shutdown(List<Delayed> delayedList) {
        this.delayedList = delayedList;
    }

    @PostConstruct
    public void init() {
        Runtime.getRuntime().addShutdownHook(this);

        if (this.delayedList != null) {
            for (Delayed delayed : this.delayedList) {
                delayed.setDelay(Constants.DEFAULT_DELAY_EXIT_MS, TimeUnit.MILLISECONDS);
            }
        }
    }

    @Override
    public void run() {
        while (true) {
            Thread thread = null;
            synchronized (waitingThreads) {
                Iterator<Thread> iterator = waitingThreads.iterator();
                if (iterator.hasNext()) {
                    thread = iterator.next();
                    iterator.remove();
                }
            }

            if (thread == null) {
                break;
            }
            try {
                thread.join();
            } catch (InterruptedException ignored) {
            }
        }

        while (true) {
            Runnable r = null;
            synchronized (waitingRunnables) {
                Iterator<Runnable> iterator = waitingRunnables.iterator();
                if (iterator.hasNext()) {
                    r = iterator.next();
                    iterator.remove();
                }
            }
            if (r == null) {
                break;
            }
            try {
                r.run();
            } catch (Throwable ignored) {
            }
        }
    }

    public static void addThread(Thread thread) {
        if (thread == null) {
            return;
        }
        synchronized (waitingThreads) {
            waitingThreads.add(thread);
        }
    }

    public static void addRunnable(Runnable runnable) {
        if (runnable == null) {
            return;
        }
        synchronized (waitingRunnables) {
            waitingRunnables.add(runnable);
        }
    }
}
