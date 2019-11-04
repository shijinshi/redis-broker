package cn.shijinshi.redis.common;

/**
 * 常量池
 *
 * @author Gui Jiahai
 */
public interface Constants {

    long PING_INTERVAL_MS = 3000;

    long DEFAULT_DELAY_EXIT_MS = 60_000;

    byte[] OK_REPLY = "+OK\r\n".getBytes();

    byte[] CONNECTION_LOST = "-ERR connection lost\r\n".getBytes();

    byte[] CRLF_BYTE = new byte[]{'\r', '\n'};

    char[] CRLF_CHAR = new char[]{'\r', '\n'};

    String CRLF_STR = "\r\n";

    String PATH_SEPARATOR = "/";

    String REPLICATION_HOLD = "active";

    String SYNC_MODE = "sync";
    String REPL_MODE = "repl";

}
