package cn.shijinshi.redis.common.error;

/**
 * 用于处理严重异常。
 *
 * 比如，在同步任务过程中，如果磁盘发生IOException，
 * 那么这时候该怎么办呢？是停止当前进程，还是继续任务？
 *
 * 很遗憾，并没有很好的解决方案，所以通过扩展此类，
 * 由开发人员根据业务需要自行处理这类异常。
 *
 * @author Gui Jiahai
 */
public class ErrorHandler {

    private static Handler handler;

    public static void handle(Throwable t) {
        Handler h;
        if ((h = handler) != null) {
            h.handler(t);
        }
    }

    public static void setHandler(Handler handler) {
        ErrorHandler.handler = handler;
    }

    /**
     * 处理异常的接口，用户可根据此接口自定义处理异常的实现逻辑
     */
    public interface Handler {
        void handler(Throwable t);
    }

}
