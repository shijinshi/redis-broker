package cn.shijinshi.redis.common.adapt;

/**
 * 在使用ReplicatorHandler处理Event时，可能会发生错误。
 * 那么该如何处理这些错误呢？重发或者忽略？
 *
 * 例如，如果发生连接断开的错误，则应该重试；
 * 如果发生语法错误，则应该忽略。
 *
 * 所以，ReplicatorHandler的实现类，应该对异常进行分类处理，
 * 如果该异常适合重发，则应该用AccessibleException对异常进行封装。
 *
 * ReplicatorHandler的使用者如果发现AccessibleException，则应该重发，
 * 否则，直接忽略该异常。
 *
 * @author Gui Jiahai
 */
public class AccessibleException extends RuntimeException {

    private final boolean disconnected;

    public AccessibleException(Throwable cause, boolean disconnected) {
        super(cause);
        this.disconnected = disconnected;
    }

    public boolean isDisconnected() {
        return disconnected;
    }
}
