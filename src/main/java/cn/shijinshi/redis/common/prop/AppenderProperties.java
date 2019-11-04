package cn.shijinshi.redis.common.prop;

import cn.shijinshi.redis.common.log.LogProperties;

/**
 * @author Gui Jiahai
 */
public class AppenderProperties {

    public static final String ASYNC_TYPE = "async";
    public static final String SYNC_TYPE = "sync";
    public static final String NO_TYPE = "no";

    private String type = ASYNC_TYPE;

    private final LogProperties log = new LogProperties();

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public LogProperties getLog() {
        return log;
    }
}
