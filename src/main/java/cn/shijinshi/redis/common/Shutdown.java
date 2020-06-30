package cn.shijinshi.redis.common;

import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * 用于优雅停机
 *
 * @author Gui Jiahai
 */
@Component
public class Shutdown extends Thread {

    private final static Set<Thread> waitingThreads = new HashSet<>();
    private final static Set<OrderRunner> waitingRunners = new TreeSet<>();

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
        for (;;)  {
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

        for (;;)  {
            Runnable r = null;
            synchronized (waitingRunners) {
                Iterator<OrderRunner> iterator = waitingRunners.iterator();
                if (iterator.hasNext()) {
                    r = iterator.next().runner;
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

    public static void addRunner(Runnable runner, int order) {
        Objects.requireNonNull(runner);
        synchronized (waitingRunners) {
            waitingRunners.add(new OrderRunner(runner, order));
        }
    }

    static class OrderRunner implements Comparable<OrderRunner> {

        final Runnable runner;
        final int order;

        OrderRunner(Runnable runner, int order) {
            this.runner = runner;
            this.order = order;
        }

        @Override
        public int compareTo(OrderRunner o) {
            return order - o.order;
        }
    }

}
