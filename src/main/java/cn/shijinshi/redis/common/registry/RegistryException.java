package cn.shijinshi.redis.common.registry;

/**
 * 操作注册中心时发生的异常
 *
 * @author Gui Jiahai
 */
@SuppressWarnings("unused")
public class RegistryException extends Exception {

    public RegistryException() {
        super();
    }

    public RegistryException(String message) {
        super(message);
    }

    public RegistryException(String message, Throwable cause) {
        super(message, cause);
    }

    public RegistryException(Throwable cause) {
        super(cause);
    }

}
