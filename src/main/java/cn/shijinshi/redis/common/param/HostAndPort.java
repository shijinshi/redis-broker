package cn.shijinshi.redis.common.param;

import java.io.Serializable;
import java.util.Objects;

/**
 * @author Gui Jiahai
 */
public final class HostAndPort implements Serializable {

    private final String host;
    private final int port;

    private transient String format;

    public HostAndPort() {
        this(null, 0);
    }

    public HostAndPort(String string) {
        com.google.common.net.HostAndPort hostAndPort = com.google.common.net.HostAndPort.fromString(string);
        this.host = hostAndPort.getHostText();
        this.port = hostAndPort.getPort();
    }

    public HostAndPort(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public final String getHost() {
        return host;
    }

    public final int getPort() {
        return port;
    }

    @Override
    public int hashCode() {
        int result = host != null ? host.hashCode() : 0;
        result = 31 * result + port;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof HostAndPort)) {
            return false;
        }
        HostAndPort that = (HostAndPort) obj;
        if (port != that.port) {
            return false;
        }
        return Objects.equals(host, that.host);
    }

    @Override
    public String toString() {
        return format();
    }

    public String format() {
        if (format == null) {
            format = this.host + ":" + this.port;
        }
        return format;
    }

    public static HostAndPort create(String host, int port) {
        return new HostAndPort(host, port);
    }

    public static HostAndPort create(String hostPortString) {
        com.google.common.net.HostAndPort hostAndPort = com.google.common.net.HostAndPort.fromString(hostPortString);
        return create(hostAndPort.getHostText(), hostAndPort.getPort());
    }

 }
