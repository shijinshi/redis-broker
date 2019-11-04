package cn.shijinshi.redis.common.adapt;

import com.moilioncircle.redis.replicator.cmd.impl.*;
import com.moilioncircle.redis.replicator.rdb.datatype.*;
import io.lettuce.core.*;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.cluster.PartitionException;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Future;
import java.util.function.BiFunction;

import static com.moilioncircle.redis.replicator.rdb.datatype.ExpiredType.MS;
import static com.moilioncircle.redis.replicator.rdb.datatype.ExpiredType.SECOND;

/**
 * ReplicatorHandler的实现类，
 * 将 {@link com.moilioncircle.redis.replicator.event.Event} 转化为lettuce可识别的对象，
 * 最终发送到redis中
 *
 * @author Gui Jiahai
 */
public class LettuceHandler extends AbstractLettuceHandler {

    private final RedisClusterAsyncCommands<byte[], byte[]> client;
    private Closeable closeable;

    public LettuceHandler(RedisClusterAsyncCommands<byte[], byte[]> client, Closeable closeable) {
        this.client = Objects.requireNonNull(client);
        this.closeable = closeable;
    }

    @Override
    public void close() throws IOException {
        if (closeable != null) {
            closeable.close();
        }
    }

    private Future<Void> toVoid(RedisFuture<?> future) {
        if (future == null) {
            return CompletableFuture.completedFuture(null);
        }
        return future
                .handle((BiFunction<Object, Throwable, Void>) (o, throwable) -> {
                    if (throwable != null) {
                        if (throwable instanceof RedisException) {
                            RedisException ex = (RedisException) throwable;
                            if (isDisconnected(ex)) {
                                throw new AccessibleException(ex, true);
                            } else if (isAccessible(ex)) {
                                throw new AccessibleException(ex, false);
                            }
                        }
                        if (throwable instanceof RuntimeException) {
                            throw (RuntimeException) throwable;
                        } else {
                            throw new CompletionException(throwable);
                        }
                    }

                    return null;
                }).toCompletableFuture();
    }

    /**
     * 参考 {@link AccessibleException}
     */
    private boolean isAccessible(RedisException ex) {
        return ex instanceof RedisConnectionException
                || ex instanceof PartitionException
                || ex instanceof RedisLoadingException
                || ex instanceof RedisBusyException;
    }

    private boolean isDisconnected(RedisException ex) {
        return ex instanceof RedisConnectionException;
    }

    /***********************  KeyValuePair *************************/

    @Override
    public Future<Void> handle(KeyStringValueString pair) {
        SetArgs args = args(pair);
        RedisFuture<String> future = client.set(pair.getKey(), pair.getValue(), args);
        return toVoid(future);
    }

    @Override
    public Future<Void> handle(KeyStringValueList pair) {
        byte[][] value = pair.getValue().toArray(new byte[pair.getValue().size()][]);
        RedisFuture<Long> future = client.rpush(pair.getKey(), value);
        RedisFuture<Boolean> expireFuture;
        if ((expireFuture = expire(pair)) != null) {
            return future.thenCombine(expireFuture, (BiFunction<Long, Boolean, Void>) (aLong, aBoolean) -> null).toCompletableFuture();
        }
        return toVoid(future);
    }

    @Override
    public Future<Void> handle(KeyStringValueHash pair) {
        RedisFuture<String> future = client.hmset(pair.getKey(), pair.getValue());
        RedisFuture<Boolean> expireFuture;
        if ((expireFuture = expire(pair)) != null) {
            return future.thenCombine(expireFuture, (BiFunction<String, Boolean, Void>) (s, aBoolean) -> null).toCompletableFuture();
        }
        return toVoid(future);
    }

    @Override
    public Future<Void> handle(KeyStringValueSet pair) {
        byte[][] value = pair.getValue().toArray(new byte[pair.getValue().size()][]);
        RedisFuture<Long> future = client.sadd(pair.getKey(), value);
        RedisFuture<Boolean> expireFuture;
        if ((expireFuture = expire(pair)) != null) {
            return future.thenCombine(expireFuture, (BiFunction<Long, Boolean, Void>) (s, aBoolean) -> null).toCompletableFuture();
        }
        return toVoid(future);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Future<Void> handle(KeyStringValueZSet pair) {
        ScoredValue<byte[]>[] values = new ScoredValue[pair.getValue().size()];
        int pos = 0;
        for (ZSetEntry entry : pair.getValue()) {
            values[pos ++] = ScoredValue.just(entry.getScore(), entry.getElement());
        }
        RedisFuture<Long> future = client.zadd(pair.getKey(), values);
        RedisFuture<Boolean> expireFuture;
        if ((expireFuture = expire(pair)) != null) {
            return future.thenCombine(expireFuture, (BiFunction<Long, Boolean, Void>) (s, aBoolean) -> null).toCompletableFuture();
        }
        return toVoid(future);
    }

    private RedisFuture<Boolean> expire(KeyValuePair<byte[], ?> pair) {
        if (pair.getExpiredType() == SECOND) {
            return client.expire(pair.getKey(), pair.getExpiredSeconds());
        } else if (pair.getExpiredType() == MS) {
            return client.pexpire(pair.getKey(), pair.getExpiredMs());
        }
        return null;
    }


    /**************************  COMMAND  ************************************/

    @Override
    public Future<Void> handle(AppendCommand command) {
        return toVoid(client.append(command.getKey(), command.getValue()));
    }

    @Override
    public Future<Void> handle(BitFieldCommand command) {
        return toVoid(client.bitfield(command.getKey(), args(command)));
    }

    @Override
    public Future<Void> handle(BitOpCommand command) {
        RedisFuture<?> future = null;
        switch (command.getOp()) {
            case OR:
                future = client.bitopOr(command.getDestkey(), command.getKeys());
                break;
            case AND:
                future = client.bitopAnd(command.getDestkey(), command.getKeys());
                break;
            case NOT:
                future = client.bitopNot(command.getDestkey(), command.getKeys()[0]);
                break;
            case XOR:
                future = client.bitopXor(command.getDestkey(), command.getKeys());
                break;
        }
        return toVoid(future);
    }

    @Override
    public Future<Void> handle(BRPopLPushCommand command) {
        return toVoid(client.brpoplpush(command.getTimeout(), command.getSource(), command.getDestination()));
    }

    @Override
    public Future<Void> handle(DecrByCommand command) {
        return toVoid(client.decrby(command.getKey(), command.getValue()));
    }

    @Override
    public Future<Void> handle(DecrCommand command) {
        return toVoid(client.decr(command.getKey()));
    }

    @Override
    public Future<Void> handle(DelCommand command) {
        return toVoid(client.del(command.getKeys()));
    }

    @Override
    public Future<Void> handle(EvalCommand command) {
        return toVoid(client.eval(new String(command.getScript()), ScriptOutputType.VALUE, command.getKeys(), command.getArgs()));
    }

    @Override
    public Future<Void> handle(EvalShaCommand command) {
        return toVoid(client.evalsha(new String(command.getSha()), ScriptOutputType.VALUE, command.getKeys(), command.getArgs()));
    }

    @Override
    public Future<Void> handle(ExecCommand command) {
        if (client instanceof RedisAsyncCommands) {
            return toVoid(((RedisAsyncCommands<byte[], byte[]>) client).exec());
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public Future<Void> handle(ExpireAtCommand command) {
        return toVoid(client.expireat(command.getKey(), command.getEx()));
    }

    @Override
    public Future<Void> handle(ExpireCommand command) {
        return toVoid(client.expire(command.getKey(), command.getEx()));
    }

    @Override
    public Future<Void> handle(GeoAddCommand command) {
        Object[] lngLatMember = new Object[command.getGeos().length  * 3];
        int pos = 0;
        for (Geo geo : command.getGeos()) {
            lngLatMember[pos ++] = geo.getLongitude();
            lngLatMember[pos ++] = geo.getLatitude();
            lngLatMember[pos ++] = geo.getMember();
        }
        return toVoid(client.geoadd(command.getKey(), lngLatMember));
    }

    @Override
    public Future<Void> handle(GetSetCommand command) {
        return toVoid(client.getset(command.getKey(), command.getValue()));
    }

    @Override
    public Future<Void> handle(HDelCommand command) {
        return toVoid(client.hdel(command.getKey(), command.getFields()));
    }

    @Override
    public Future<Void> handle(HIncrByCommand command) {
        return toVoid(client.hincrby(command.getKey(), command.getField(), command.getIncrement()));
    }

    @Override
    public Future<Void> handle(HMSetCommand command) {
        return toVoid(client.hmset(command.getKey(), command.getFields()));
    }

    @Override
    public Future<Void> handle(HSetCommand command) {
        return toVoid(client.hset(command.getKey(), command.getField(), command.getValue()));
    }

    @Override
    public Future<Void> handle(HSetNxCommand command) {
        return toVoid(client.hsetnx(command.getKey(), command.getField(), command.getValue()));
    }

    @Override
    public Future<Void> handle(IncrByCommand command) {
        return toVoid(client.incrby(command.getKey(), command.getValue()));
    }

    @Override
    public Future<Void> handle(IncrCommand command) {
        return toVoid(client.incr(command.getKey()));
    }

    @Override
    public Future<Void> handle(LInsertCommand command) {
        return toVoid(client.linsert(command.getKey(), command.getlInsertType() == LInsertType.BEFORE, command.getPivot(), command.getValue()));
    }

    @Override
    public Future<Void> handle(LPopCommand command) {
        return toVoid(client.lpop(command.getKey()));
    }

    @Override
    public Future<Void> handle(LPushCommand command) {
        return toVoid(client.lpush(command.getKey(), command.getValues()));
    }

    @Override
    public Future<Void> handle(LPushXCommand command) {
        return toVoid(client.lpushx(command.getKey(), command.getValues()));
    }

    @Override
    public Future<Void> handle(LRemCommand command) {
        return toVoid(client.lrem(command.getKey(), command.getIndex(), command.getValue()));
    }

    @Override
    public Future<Void> handle(LSetCommand command) {
        return toVoid(client.lset(command.getKey(), command.getIndex(), command.getValue()));
    }

    @Override
    public Future<Void> handle(LTrimCommand command) {
        return toVoid(client.ltrim(command.getKey(), command.getStart(), command.getStop()));
    }

    @Override
    public Future<Void> handle(MoveCommand command) {
        return toVoid(client.move(command.getKey(), command.getDb()));
    }

    @Override
    public Future<Void> handle(MSetCommand command) {
        return toVoid(client.mset(command.getKv()));
    }

    @Override
    public Future<Void> handle(MSetNxCommand command) {
        return toVoid(client.msetnx(command.getKv()));
    }

    @Override
    public Future<Void> handle(MultiCommand command) {
        if (client instanceof RedisAsyncCommands) {
            return toVoid(((RedisAsyncCommands<byte[], byte[]>) client).multi());
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public Future<Void> handle(PersistCommand command) {
        return toVoid(client.persist(command.getKey()));
    }

    @Override
    public Future<Void> handle(PExpireAtCommand command) {
        return toVoid(client.pexpireat(command.getKey(), command.getEx()));
    }

    @Override
    public Future<Void> handle(PExpireCommand command) {
        return toVoid(client.pexpire(command.getKey(), command.getEx()));
    }

    @Override
    public Future<Void> handle(PFAddCommand command) {
        return toVoid(client.pfadd(command.getKey(), command.getElements()));
    }

    @Override
    public Future<Void> handle(PFCountCommand command) {
        return toVoid(client.pfcount(command.getKeys()));
    }

    @Override
    public Future<Void> handle(PFMergeCommand command) {
        return toVoid(client.pfmerge(command.getDestkey(), command.getSourcekeys()));
    }

    @Override
    public Future<Void> handle(PSetExCommand command) {
        return toVoid(client.psetex(command.getKey(), command.getEx(), command.getValue()));
    }

    @Override
    public Future<Void> handle(RenameCommand command) {
        return toVoid(client.rename(command.getKey(), command.getNewKey()));
    }

    @Override
    public Future<Void> handle(RenameNxCommand command) {
        return toVoid(client.renamenx(command.getKey(), command.getNewKey()));
    }

    @Override
    public Future<Void> handle(RestoreCommand command) {
        return toVoid(client.restore(command.getKey(), command.getSerializedValue(), args(command)));
    }

    @Override
    public Future<Void> handle(RPopCommand command) {
        return toVoid(client.rpop(command.getKey()));
    }

    @Override
    public Future<Void> handle(RPopLPushCommand command) {
        return toVoid(client.rpoplpush(command.getSource(), command.getDestination()));
    }

    @Override
    public Future<Void> handle(RPushCommand command) {
        return toVoid(client.rpush(command.getKey(), command.getValues()));
    }

    @Override
    public Future<Void> handle(RPushXCommand command) {
        return toVoid(client.rpushx(command.getKey(), command.getValues()));
    }

    @Override
    public Future<Void> handle(SAddCommand command) {
        return toVoid(client.sadd(command.getKey(), command.getMembers()));
    }

    @Override
    public Future<Void> handle(ScriptLoadCommand command) {
        return toVoid(client.scriptLoad(command.getScript()));
    }

    @Override
    public Future<Void> handle(SDiffStoreCommand command) {
        return toVoid(client.sdiffstore(command.getDestination(), command.getKeys()));
    }

    @Override
    public Future<Void> handle(SelectCommand command) {
        if (client instanceof RedisAsyncCommands) {
            ((RedisAsyncCommands<byte[], byte[]>) client).select(command.getIndex());
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public Future<Void> handle(SetBitCommand command) {
        return toVoid(client.setbit(command.getKey(), command.getOffset(), command.getValue()));
    }

    @Override
    public Future<Void> handle(SetCommand command) {
        return toVoid(client.set(command.getKey(), command.getValue(), args(command)));
    }

    @Override
    public Future<Void> handle(SetExCommand command) {
        return toVoid(client.setex(command.getKey(), command.getEx(), command.getValue()));
    }

    @Override
    public Future<Void> handle(SetNxCommand command) {
        return toVoid(client.setnx(command.getKey(), command.getValue()));
    }

    @Override
    public Future<Void> handle(SetRangeCommand command) {
        return toVoid(client.setrange(command.getKey(), command.getIndex(), command.getValue()));
    }

    @Override
    public Future<Void> handle(SInterStoreCommand command) {
        return toVoid(client.sinterstore(command.getDestination(), command.getKeys()));
    }

    @Override
    public Future<Void> handle(SMoveCommand command) {
        return toVoid(client.smove(command.getSource(), command.getDestination(), command.getMember()));
    }

    @Override
    public Future<Void> handle(SortCommand command) {
        if (command.getDestination() != null && command.getDestination().length != 0) {
            return toVoid(client.sortStore(command.getKey(), args(command), command.getDestination()));
        } else {
            return toVoid(client.sort(command.getKey(), args(command)));
        }
    }

    @Override
    public Future<Void> handle(SRemCommand command) {
        return toVoid(client.srem(command.getKey(), command.getMembers()));
    }

    @Override
    public Future<Void> handle(SUnionStoreCommand command) {
        return toVoid(client.sunionstore(command.getDestination(), command.getKeys()));
    }

    @Override
    public Future<Void> handle(SwapDBCommand command) {
        if (client instanceof RedisAsyncCommands) {
            return toVoid(((RedisAsyncCommands<byte[], byte[]>) client).swapdb(command.getSource(), command.getTarget()));
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public Future<Void> handle(UnLinkCommand command) {
        return toVoid(client.unlink(command.getKeys()));
    }

    @Override
    public Future<Void> handle(ZAddCommand command) {
        Object[] scoresAndValues = new Object[command.getZSetEntries().length * 2];
        int pos = 0;
        for (ZSetEntry entry : command.getZSetEntries()) {
            scoresAndValues[pos ++] = entry.getScore();
            scoresAndValues[pos ++] = entry.getElement();
        }
        return toVoid(client.zadd(command.getKey(), args(command), scoresAndValues));
    }

    @Override
    public Future<Void> handle(ZIncrByCommand command) {
        return toVoid(client.zincrby(command.getKey(), command.getIncrement(), command.getMember()));
    }

    @Override
    public Future<Void> handle(ZInterStoreCommand command) {
        return toVoid(client.zinterstore(command.getDestination(), args(command), command.getKeys()));
    }

    @Override
    public Future<Void> handle(ZPopMaxCommand command) {
        return toVoid(client.zpopmax(command.getKey(), command.getCount()));
    }

    @Override
    public Future<Void> handle(ZPopMinCommand command) {
        return toVoid(client.zpopmin(command.getKey(), command.getCount()));
    }

    @Override
    public Future<Void> handle(ZRemCommand command) {
        return toVoid(client.zrem(command.getKey(), command.getMembers()));
    }

    @Override
    public Future<Void> handle(ZRemRangeByLexCommand command) {
        return toVoid(client.zremrangebylex(command.getKey(),
                Range.from(boundaryForLex(command.getMin()), boundaryForLex(command.getMax()))));
    }

    @Override
    public Future<Void> handle(ZRemRangeByRankCommand command) {
        return toVoid(client.zremrangebyrank(command.getKey(), command.getStart(), command.getStop()));
    }

    @Override
    public Future<Void> handle(ZRemRangeByScoreCommand command) {
        return toVoid(client.zremrangebyscore(command.getKey(),
                Range.from(boundaryForScore(command.getMin()), boundaryForScore(command.getMax()))));
    }

    @Override
    public Future<Void> handle(ZUnionStoreCommand command) {
        return toVoid(client.zunionstore(command.getDestination(), args(command), command.getKeys()));
    }


}
