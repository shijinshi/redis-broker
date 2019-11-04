package cn.shijinshi.redis.common.adapt;

import com.moilioncircle.redis.replicator.cmd.impl.*;
import com.moilioncircle.redis.replicator.rdb.datatype.ExpiredType;
import com.moilioncircle.redis.replicator.rdb.datatype.KeyStringValueString;
import io.lettuce.core.*;

import java.util.Arrays;

/**
 * @author Gui Jiahai
 */
abstract class AbstractLettuceHandler implements ReplicatorHandler {

    SetArgs args(KeyStringValueString pair) {
        SetArgs args = new SetArgs();
        if (pair.getExpiredType() == ExpiredType.MS) {
            args.px(pair.getExpiredMs());
        } else if (pair.getExpiredType() == ExpiredType.SECOND) {
            args.ex(pair.getExpiredSeconds());
        }
        return args;
    }

    SetArgs args(SetCommand command) {
        SetArgs args = new SetArgs();
        if (command.getExistType() == ExistType.NX) {
            args.nx();
        } else if (command.getExistType() == ExistType.XX) {
            args.xx();
        }
        if (command.getExpiredType() == ExpiredType.MS) {
            args.px(command.getExpiredValue());
        } else if (command.getExpiredType() == ExpiredType.SECOND) {
            args.ex(command.getExpiredValue());
        }
        return args;
    }

    RestoreArgs args(RestoreCommand command) {
        return new RestoreArgs()
                .replace(command.isReplace())
                .ttl(command.getTtl());
    }

    BitFieldArgs args(BitFieldCommand command) {
        BitFieldArgs args = new BitFieldArgs();
        for (Statement statement: command.getStatements()) {
            subArgs(statement, args);
        }
        for (OverFlow flow: command.getOverFlows()) {
            args.overflow(BitFieldArgs.OverflowType.valueOf(flow.getOverFlowType().name().toUpperCase()));
            for (Statement statement: flow.getStatements()) {
                subArgs(statement, args);
            }
        }
        return args;
    }

    private void subArgs(Statement statement, BitFieldArgs args) {
        if (statement instanceof GetTypeOffset) {
            GetTypeOffset state = (GetTypeOffset) statement;
            args.get(bitFieldType(state.getType()), Integer.parseInt(new String(state.getOffset())));

        } else if (statement instanceof SetTypeOffsetValue) {
            SetTypeOffsetValue state = (SetTypeOffsetValue) statement;
            args.set(bitFieldType(state.getType()), Integer.parseInt(new String(state.getOffset())), state.getValue());

        } else if (statement instanceof IncrByTypeOffsetIncrement) {
            IncrByTypeOffsetIncrement state = (IncrByTypeOffsetIncrement) statement;
            args.incrBy(bitFieldType(state.getType()), Integer.parseInt(new String(state.getOffset())), state.getIncrement());
        }
    }

    private BitFieldArgs.BitFieldType bitFieldType(byte[] bytes) {
        if (bytes[0] == 'i') {
            return BitFieldArgs.signed(Integer.parseInt(new String(bytes, 1, bytes.length - 1)));
        } else {
            return BitFieldArgs.unsigned(Integer.parseInt(new String(bytes, 1, bytes.length - 1)));
        }
    }

    SortArgs args(SortCommand command) {
        SortArgs args = new SortArgs();
        if (command.getByPattern() != null && command.getByPattern().length != 0) {
            args.by(new String(command.getByPattern()));
        }
        if (command.getLimit() != null) {
            args.limit(command.getLimit().getOffset(), command.getLimit().getCount());
        }
        if (command.getGetPatterns() != null && command.getGetPatterns().length != 0) {
            Arrays.stream(command.getGetPatterns()).forEach(bytes -> args.get(new String(bytes)));
        }
        if (command.getOrder() == OrderType.ASC) {
            args.asc();
        } else if (command.getOrder() == OrderType.DESC) {
            args.desc();
        }
        if (command.isAlpha()) {
            args.alpha();
        }
        return args;
    }

    ZAddArgs args(ZAddCommand command) {
        ZAddArgs args = new ZAddArgs();
        if (command.isCh()) {
            args.ch();
        }
        if (command.getExistType() == ExistType.NX) {
            args.nx();
        } else if (command.getExistType() == ExistType.XX) {
            args.xx();
        }
        return args;
    }

    ZStoreArgs args(ZInterStoreCommand command) {
        ZStoreArgs args = new ZStoreArgs();
        args.weights(command.getWeights());
        aggregate(args, command.getAggregateType());
        return args;
    }

    Range.Boundary<byte[]> boundaryForLex(byte[] bytes) {
        switch (bytes[0]) {
            case '-':
            case '+':
                return Range.Boundary.unbounded();
            case '(':
                return Range.Boundary.excluding(Arrays.copyOfRange(bytes, 1, bytes.length));
            case '[':
                return Range.Boundary.including(Arrays.copyOfRange(bytes, 1, bytes.length));
        }
        throw new IllegalArgumentException("unknown boundaryForLex: " + new String(bytes));
    }

    Range.Boundary<Number> boundaryForScore(byte[] bytes) {
        switch (bytes[0]) {
            case '-':
            case '+':
                return Range.Boundary.unbounded();
            case '(':
                return Range.Boundary.excluding(Double.parseDouble(new String(bytes, 1, bytes.length)));
            case '[':
                return Range.Boundary.including(Double.parseDouble(new String(bytes, 1, bytes.length)));
        }
        throw new IllegalArgumentException("unknown boundaryForLex: " + new String(bytes));
    }

    ZStoreArgs args(ZUnionStoreCommand command) {
        ZStoreArgs args = new ZStoreArgs();
        args.weights(command.getWeights());
        aggregate(args, command.getAggregateType());
        return args;
    }

    private void aggregate(ZStoreArgs args, AggregateType type) {
        switch (type) {
            case MAX:
                args.max();
                break;
            case MIN:
                args.min();
                break;
            case SUM:
                args.sum();
                break;
        }
    }

}
