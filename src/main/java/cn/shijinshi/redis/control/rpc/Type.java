package cn.shijinshi.redis.control.rpc;

/**
 * @author Gui Jiahai
 */
public enum Type {

    PING,
    CLUSTER,

    SYNC_RUN,
    SYNC_PAUSE,
    SYNC_DISCARD,
    SYNC_PING,

    REPL_ACTIVE,
    REPL_PING,
    REPL_IN_COMPLETION,
    REPL_INTERRUPT

}
