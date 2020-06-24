package cn.shijinshi.redis.forward.support;

import cn.shijinshi.redis.common.util.UnsafeByteString;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Gui Jiahai
 */
@Component
@Lazy
public class CommandSupports {

    //增大HashMap的容量，降低负载因子，从而降低碰撞率
    private final Map<UnsafeByteString, Support> supports
            = new HashMap<>(256, 0.5f);

    private final Map<String, Action> actionMap;

    public CommandSupports(Map<String, Action> actionMap) {
        this.actionMap = actionMap;
    }

    public Support get(UnsafeByteString command) {
        return supports.get(command);
    }

    private void addSupport(String command, boolean backup) {
        supports.put(new UnsafeByteString(command), new Support(backup, actionMap.get(command)));
    }

    @PostConstruct
    public void init() {
        addSupport("set", true);
        addSupport("get", false);
        addSupport("exists", true);
        addSupport("del", true);
        addSupport("type", false);
        addSupport("expire", true);
        addSupport("expireat", true);
        addSupport("ttl", false);
        addSupport("getset", true);
        addSupport("mget", false);
        addSupport("setnx", true);
        addSupport("setex", true);
        addSupport("decrby", true);
        addSupport("decr", true);
        addSupport("incrby", true);
        addSupport("incr", true);
        addSupport("append", true);
        addSupport("substr", false);
        addSupport("hset", true);
        addSupport("hget", false);
        addSupport("hsetnx", true);
        addSupport("hmset", true);
        addSupport("hmget", false);
        addSupport("hincrby", true);
        addSupport("hexists", false);
        addSupport("hdel", true);
        addSupport("hlen", false);
        addSupport("hkeys", false);
        addSupport("hvals", false);
        addSupport("hgetall", false);
        addSupport("rpush", true);
        addSupport("lpush", true);
        addSupport("llen", false);
        addSupport("lrange", false);
        addSupport("ltrim", true);
        addSupport("lindex", false);
        addSupport("lset", true);
        addSupport("lrem", true);
        addSupport("lpop", true);
        addSupport("rpop", true);
        addSupport("sadd", true);
        addSupport("smembers", false);
        addSupport("srem", true);
        addSupport("spop", true);
        addSupport("scard", false);
        addSupport("sismember", false);
        addSupport("srandmember", false);
        addSupport("zadd", true);
        addSupport("zrange", false);
        addSupport("zrem", true);
        addSupport("zincrby", true);
        addSupport("zrank", false);
        addSupport("zrevrank", false);
        addSupport("zrevrange", false);
        addSupport("zcard", false);
        addSupport("zscore", false);
        addSupport("sort", false);
        addSupport("blpop", true);
        addSupport("brpop", true);
        addSupport("zcount", false);
        addSupport("zrangebyscore", false);
        addSupport("zrevrangebyscore", false);
        addSupport("zremrangebyrank", true);
        addSupport("zremrangebyscore", true);
        addSupport("zlexcount", false);
        addSupport("zrangebylex", false);
        addSupport("zrevrangebylex", false);
        addSupport("zremrangebylex", true);
        addSupport("strlen", false);
        addSupport("lpushx", true);
        addSupport("persist", true);
        addSupport("rpushx", true);
        addSupport("echo", false);
        addSupport("linsert", true);
        addSupport("setbit", true);
        addSupport("getbit", false);
        addSupport("bitpos", false);
        addSupport("setrange", true);
        addSupport("getrange", false);
        addSupport("bitcount", false);
        addSupport("pexpire", true);
        addSupport("pexpireat", true);
        addSupport("pttl", false);
        addSupport("incrbyfloat", true);
        addSupport("psetex", true);
        addSupport("hincrbyfloat", true);
        addSupport("hscan", false);
        addSupport("sscan", false);
        addSupport("zscan", false);
        addSupport("pfadd", true);
        addSupport("pfcount", false);
        addSupport("select", false);
        addSupport("keys", false);
        addSupport("config", false);

        addSupport("command", false);
        addSupport("eval", true);
        addSupport("info", false);

        addSupport("ping", false);
        addSupport("cluster", false);
        addSupport("client", false);
        addSupport("quit", false);

        addSupport("rpc", false);
    }



}
