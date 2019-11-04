package cn.shijinshi.redis.common.adapt;

import com.moilioncircle.redis.replicator.cmd.impl.*;
import com.moilioncircle.redis.replicator.event.PostCommandSyncEvent;
import com.moilioncircle.redis.replicator.event.PostRdbSyncEvent;
import com.moilioncircle.redis.replicator.event.PreCommandSyncEvent;
import com.moilioncircle.redis.replicator.event.PreRdbSyncEvent;
import com.moilioncircle.redis.replicator.rdb.datatype.*;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * 使用 {@link com.moilioncircle.redis.replicator.Replicator} 进行解析Redis报文时，
 * 此接口用于处理 {@link com.moilioncircle.redis.replicator.event.Event}
 *
 * @author Gui Jiahai
 */
@SuppressWarnings("unused")
public interface ReplicatorHandler extends Closeable {

    Future<Void> handle(KeyStringValueString pair);
    Future<Void> handle(KeyStringValueList pair);
    Future<Void> handle(KeyStringValueHash pair);
    Future<Void> handle(KeyStringValueSet pair);
    Future<Void> handle(KeyStringValueZSet pair);

    Future<Void> handle(AppendCommand command);
    Future<Void> handle(BitFieldCommand command);
    Future<Void> handle(BitOpCommand command);
    Future<Void> handle(BRPopLPushCommand command);
    Future<Void> handle(DecrByCommand command);
    Future<Void> handle(DecrCommand command);
    Future<Void> handle(DelCommand command);
    Future<Void> handle(EvalCommand command);
    Future<Void> handle(EvalShaCommand command);
    Future<Void> handle(ExecCommand command);
    Future<Void> handle(ExpireAtCommand command);
    Future<Void> handle(ExpireCommand command);
    Future<Void> handle(GeoAddCommand command);
    Future<Void> handle(GetSetCommand command);
    Future<Void> handle(HDelCommand command);
    Future<Void> handle(HIncrByCommand command);
    Future<Void> handle(HMSetCommand command);
    Future<Void> handle(HSetCommand command);
    Future<Void> handle(HSetNxCommand command);
    Future<Void> handle(IncrByCommand command);
    Future<Void> handle(IncrCommand command);
    Future<Void> handle(LInsertCommand command);
    Future<Void> handle(LPopCommand command);
    Future<Void> handle(LPushCommand command);
    Future<Void> handle(LPushXCommand command);
    Future<Void> handle(LRemCommand command);
    Future<Void> handle(LSetCommand command);
    Future<Void> handle(LTrimCommand command);
    Future<Void> handle(MoveCommand command);
    Future<Void> handle(MSetCommand command);
    Future<Void> handle(MSetNxCommand command);
    Future<Void> handle(MultiCommand command);
    Future<Void> handle(PersistCommand command);
    Future<Void> handle(PExpireAtCommand command);
    Future<Void> handle(PExpireCommand command);
    Future<Void> handle(PFAddCommand command);
    Future<Void> handle(PFCountCommand command);
    Future<Void> handle(PFMergeCommand command);
    Future<Void> handle(PSetExCommand command);
    Future<Void> handle(RenameCommand command);
    Future<Void> handle(RenameNxCommand command);
    Future<Void> handle(RestoreCommand command);
    Future<Void> handle(RPopCommand command);
    Future<Void> handle(RPopLPushCommand command);
    Future<Void> handle(RPushCommand command);
    Future<Void> handle(RPushXCommand command);
    Future<Void> handle(SAddCommand command);
    Future<Void> handle(ScriptLoadCommand command);
    Future<Void> handle(SDiffStoreCommand command);
    Future<Void> handle(SelectCommand command);
    Future<Void> handle(SetBitCommand command);
    Future<Void> handle(SetCommand command);
    Future<Void> handle(SetExCommand command);
    Future<Void> handle(SetNxCommand command);
    Future<Void> handle(SetRangeCommand command);
    Future<Void> handle(SInterStoreCommand command);
    Future<Void> handle(SMoveCommand command);
    Future<Void> handle(SortCommand command);
    Future<Void> handle(SRemCommand command);
    Future<Void> handle(SUnionStoreCommand command);
    Future<Void> handle(SwapDBCommand command);
    Future<Void> handle(UnLinkCommand command);
    Future<Void> handle(ZAddCommand command);
    Future<Void> handle(ZIncrByCommand command);
    Future<Void> handle(ZInterStoreCommand command);
    Future<Void> handle(ZPopMaxCommand command);
    Future<Void> handle(ZPopMinCommand command);
    Future<Void> handle(ZRemCommand command);
    Future<Void> handle(ZRemRangeByLexCommand command);
    Future<Void> handle(ZRemRangeByRankCommand command);
    Future<Void> handle(ZRemRangeByScoreCommand command);
    Future<Void> handle(ZUnionStoreCommand command);

    default Future<Void> handle(AuxField event) {
        return CompletableFuture.completedFuture(null);
    }

    default Future<Void> handle(PreRdbSyncEvent command) {
        return CompletableFuture.completedFuture(null);
    }

    default Future<Void> handle(PostRdbSyncEvent command) {
        return CompletableFuture.completedFuture(null);
    }

    default Future<Void> handle(PreCommandSyncEvent command) {
        return CompletableFuture.completedFuture(null);
    }

    default Future<Void> handle(PostCommandSyncEvent command) {
        return CompletableFuture.completedFuture(null);
    }

    default Future<Void> handle(DefaultCommand command) {
        return CompletableFuture.completedFuture(null);
    }

    default Future<Void> handle(FlushAllCommand command) {
        return CompletableFuture.completedFuture(null);
    }

    default Future<Void> handle(FlushDBCommand command) {
        return CompletableFuture.completedFuture(null);
    }

    default Future<Void> handle(PingCommand command) {
        return CompletableFuture.completedFuture(null);
    }

    default Future<Void> handle(PublishCommand command) {
        return CompletableFuture.completedFuture(null);
    }

    default Future<Void> handle(ReplConfCommand command) {
        return CompletableFuture.completedFuture(null);
    }

    default Future<Void> handle(ReplConfGetAckCommand command) {
        return CompletableFuture.completedFuture(null);
    }

    default Future<Void> handle(ScriptFlushCommand command) {
        return CompletableFuture.completedFuture(null);
    }

    default Future<Void> handle(XAckCommand command) {
        return CompletableFuture.completedFuture(null);
    }

    default Future<Void> handle(XAddCommand command) {
        return CompletableFuture.completedFuture(null);
    }

    default Future<Void> handle(XClaimCommand command) {
        return CompletableFuture.completedFuture(null);
    }

    default Future<Void> handle(XDelCommand command) {
        return CompletableFuture.completedFuture(null);
    }

    default Future<Void> handle(XGroupCommand command) {
        return CompletableFuture.completedFuture(null);
    }

    default Future<Void> handle(XGroupCreateCommand command) {
        return CompletableFuture.completedFuture(null);
    }

    default Future<Void> handle(XGroupDelConsumerCommand command) {
        return CompletableFuture.completedFuture(null);
    }

    default Future<Void> handle(XGroupDestroyCommand command) {
        return CompletableFuture.completedFuture(null);
    }

    default Future<Void> handle(XGroupSetIdCommand command) {
        return CompletableFuture.completedFuture(null);
    }

    default Future<Void> handle(XSetIdCommand command) {
        return CompletableFuture.completedFuture(null);
    }

    default Future<Void> handle(XTrimCommand command) {
        return CompletableFuture.completedFuture(null);
    }
}
