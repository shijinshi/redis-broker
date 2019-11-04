package cn.shijinshi.redis.common.error;

/**
 * 处理严重异常的实现类。
 * 采取严格的策略，一旦发现异常，就结束当前进程。
 *
 * @author Gui Jiahai
 */
public class StrictErrorHandler implements ErrorHandler.Handler {

    @Override
    public void handler(Throwable t) {
        System.exit(0);
    }
}
