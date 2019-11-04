package cn.shijinshi.redis.common.error;

import cn.shijinshi.redis.common.prop.BrokerProperties;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

/**
 * @author Gui Jiahai
 */
@Component
public class ErrorHandlerConfiguration implements ApplicationContextAware {

    private static final String STRICT = "strict";
    private static final String LAX = "lax";

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        BrokerProperties properties = applicationContext.getBean(BrokerProperties.class);
        String s = properties.getErrorHandler();
        ErrorHandler.Handler handler;
        if (s == null || s.isEmpty() || STRICT.equalsIgnoreCase(s)) {
            handler = new StrictErrorHandler();
        } else if (LAX.equalsIgnoreCase(s)) {
            handler = new LaxErrorHandler();
        } else {
            throw new IllegalArgumentException("Unknown error-handler[" + s + "]");
        }
        ErrorHandler.setHandler(handler);
    }
}
