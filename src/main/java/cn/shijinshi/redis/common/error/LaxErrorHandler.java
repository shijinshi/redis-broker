package cn.shijinshi.redis.common.error;

/**
 * 处理严重异常的实现类。
 * 采取宽松的策略，对所有的异常都会忽略。
 *
 * @author Gui Jiahai
 */
public class LaxErrorHandler implements ErrorHandler.Handler {

    @Override
    public void handler(Throwable t) {

    }
}
