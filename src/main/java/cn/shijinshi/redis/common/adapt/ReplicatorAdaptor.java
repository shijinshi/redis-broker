package cn.shijinshi.redis.common.adapt;

import cn.shijinshi.redis.common.error.ErrorHandler;
import com.moilioncircle.redis.replicator.event.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Future;

/**
 * {@link ReplicatorHandler} 适配器
 *
 * @author Gui Jiahai
 */
public class ReplicatorAdaptor implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(ReplicatorAdaptor.class);

    private static final Map<Class<?>, Method> methodMap;

    static {
        Map<Class<?>, Method> map = new HashMap<>(128, 0.5f);
        Method[] methods = ReplicatorHandler.class.getDeclaredMethods();
        for (Method method : methods) {
            if (method.getParameterTypes().length == 1) {
                Class<?> c = method.getParameterTypes()[0];
                if (Event.class.isAssignableFrom(c) && Future.class.isAssignableFrom(method.getReturnType())) {
                    map.put(c, method);
                }
            }
        }
        methodMap = Collections.unmodifiableMap(map);
    }

    private final ReplicatorHandler handler;

    public ReplicatorAdaptor(ReplicatorHandler handler) {
        this.handler = Objects.requireNonNull(handler);
    }

    @SuppressWarnings("unchecked")
    public Future<Void> dispatch(Event event) {
        Method method = methodMap.get(event.getClass());
        if (method == null) {
            throw new UnsupportedOperationException("Cannot handle event type: " + event.getClass());
        }

        try {
            return (Future<Void>) method.invoke(handler, event);
        } catch (IllegalAccessException | InvocationTargetException e) {
            logger.error("Cannot access method: {}", method, e);
            ErrorHandler.handle(e);
        }
        return null;
    }

    @Override
    public void close() {
        try {
            this.handler.close();
        } catch (IOException ignored) {}
    }
}
