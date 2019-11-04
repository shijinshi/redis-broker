package cn.shijinshi.redis.common.util;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author Gui Jiahai
 */
public final class ExecutorUtil {

    private ExecutorUtil() {}

    /**
     * 优雅关闭线程池
     * @param timeout 如果希望立即关闭线程池，则设为0
     */
    public static void shutdown(ExecutorService executor, long timeout, TimeUnit timeUnit) {

        if (executor == null || executor.isTerminated()) {
            return;
        }

        if (timeout <= 0) {
            executor.shutdownNow();
            return;
        }

        executor.shutdown();
        try {
            if (!executor.awaitTermination(timeout, timeUnit)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

}
