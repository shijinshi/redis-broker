package cn.shijinshi.redis.common.prop;

import cn.shijinshi.redis.common.log.LogProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;

import javax.annotation.PostConstruct;
import java.net.InetAddress;
import java.net.UnknownHostException;

import static cn.shijinshi.redis.common.Constants.PATH_SEPARATOR;

/**
 * @author Gui Jiahai
 */
@ConfigurationProperties(prefix = "broker")
public class BrokerProperties {

    private Integer port;

    private String address;

    private String serialize;

    private String errorHandler;

    private final AppenderProperties appender = new AppenderProperties();

    private final RedisProperties source = new RedisProperties();

    private final RedisProperties target = new RedisProperties();

    private final ZookeeperProperties zookeeper = new ZookeeperProperties();

    private String replicationPath;
    private String brokersPath;

    @PostConstruct
    public void init() throws UnknownHostException {
        if (this.port == null) {
            throw new IllegalStateException("redis.port must not be null");
        }
        if (this.address == null) {
            String localhost = InetAddress.getLocalHost().getHostAddress();
            if (localhost == null || "127.0.0.1".equals(localhost) || "0.0.0.0".equals(localhost)) {
                throw new IllegalStateException("Invalid host: " + String.valueOf(localhost));
            }
            this.address = localhost;
        }

        if (!zookeeper.getRoot().startsWith(PATH_SEPARATOR)) {
            zookeeper.setRoot(PATH_SEPARATOR + zookeeper.getRoot());
        }
        if (!zookeeper.getRoot().endsWith(PATH_SEPARATOR)) {
            zookeeper.setRoot(zookeeper.getRoot() + PATH_SEPARATOR);
        }
        this.replicationPath = zookeeper.getRoot() + "replication";
        this.brokersPath = zookeeper.getRoot() + "brokers";

        LogProperties conf = this.appender.getLog();
        conf.setDir(conf.getDir() + PATH_SEPARATOR + port);
    }

    public String getReplicationPath() {
        return replicationPath;
    }

    public String getBrokersPath() {
        return brokersPath;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getSerialize() {
        return serialize;
    }

    public void setSerialize(String serialize) {
        this.serialize = serialize;
    }

    public String getErrorHandler() {
        return errorHandler;
    }

    public void setErrorHandler(String errorHandler) {
        this.errorHandler = errorHandler;
    }

    public AppenderProperties getAppender() {
        return appender;
    }

    public RedisProperties getSource() {
        return source;
    }

    public RedisProperties getTarget() {
        return target;
    }

    public ZookeeperProperties getZookeeper() {
        return zookeeper;
    }
}
