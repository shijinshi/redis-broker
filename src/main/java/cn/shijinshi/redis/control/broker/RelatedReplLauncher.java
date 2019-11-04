package cn.shijinshi.redis.control.broker;

import cn.shijinshi.redis.common.prop.BrokerProperties;
import cn.shijinshi.redis.control.rpc.Answer;
import cn.shijinshi.redis.control.rpc.Indication;
import cn.shijinshi.redis.control.rpc.Type;
import cn.shijinshi.redis.replicate.Observer;
import cn.shijinshi.redis.replicate.ReplicationJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

import static cn.shijinshi.redis.control.rpc.Type.*;

/**
 * 复制模块的启动器
 *
 * @author Gui Jiahai
 */
public class RelatedReplLauncher implements Observer<ReplicationJob>, Launcher {

    private static final Logger logger = LoggerFactory.getLogger(RelatedReplLauncher.class);

    private final BrokerProperties properties;

    private ReplicationJob replicationJob;
    private Type curState;
    private Answer next;

    public RelatedReplLauncher(BrokerProperties properties) {
        this.properties = Objects.requireNonNull(properties);
    }

    @Override
    public void start() {
    }

    @Override
    public boolean support(Indication indication) {
        Type type = indication.getType();
        return type == REPL_ACTIVE || type == REPL_PING
            || type == REPL_IN_COMPLETION || type == REPL_INTERRUPT;
    }

    @Override
    public synchronized Answer apply(Indication indication) {
        logger.info("Apply indication: {}, current state: {}", indication, curState);

        switch (indication.getType()) {
            case REPL_ACTIVE:
                return replActive();
            case REPL_PING:
                return replPing();
            case REPL_IN_COMPLETION:
                return replInCompletion(indication);
            case REPL_INTERRUPT:
                return replInterrupt();
            default:
                return Answer.bad("Unable to process indication of type: " + indication.getType());
        }
    }

    @Override
    public synchronized void onNext(ReplicationJob obj) {
        if (obj == this.replicationJob) {
            if (next != null) {
                logger.warn("The value of 'next' should be null: {}, but: ", next);
            }
            next = Answer.next();
        }
    }

    @Override
    public synchronized void onCompleted(ReplicationJob obj) {
        if (next != null) {
            logger.warn("The value of `next` should be null: {}, but: ", next);
        }
        next = Answer.next();
        clear();
    }

    @Override
    public synchronized void onError(ReplicationJob obj) {
        if (next != null) {
            logger.warn("The value of `next` should be null: {}, but: ", next);
        }
        next = Answer.bad("Replication job failed");
        clear();
    }

    @Override
    public synchronized void stop() {
        clear();
        next = null;
    }

    private void clear() {
        curState = null;
        ReplicationJob job = this.replicationJob;
        if (job != null) {
            this.replicationJob = null;
            if (job.isAlive()) {
                job.interrupt();
            }
        }
    }

    /****************** REPL ****************************************/

    private Answer replActive() {
        if (curState != null) {
            return Answer.bad("Unable to process indication of type " + REPL_ACTIVE + " cause current state is " + curState);
        }
        if (this.replicationJob != null) {
            return Answer.bad("Unable to process indication of type " + REPL_ACTIVE + " cause the replication job already existed");
        }

        clear();
        next = null;

        curState = REPL_ACTIVE;
        replicationJob = new ReplicationJob(properties.getSource(), properties.getTarget(), this);
        replicationJob.start();
        return Answer.ok();
    }

    private Answer replPing() {
        if (next != null) {
            try {
                return next;
            } finally {
                next = null;
            }
        } else  if (curState != REPL_ACTIVE && curState != REPL_IN_COMPLETION) {
            return Answer.bad("Cannot process indication of type " + REPL_PING + "cause current state is " + curState);
        } else {
            if (this.replicationJob == null || !this.replicationJob.isAlive()) {
                return Answer.bad("Cannot process indication of type " + REPL_PING + " cause replication job is disappeared");
            }
        }

        return Answer.ok();
    }

    private Answer replInCompletion(Indication indication) {
        if (curState != REPL_ACTIVE) {
            return Answer.bad("Cannot process indication of type " + REPL_IN_COMPLETION + "cause current state is " + curState);
        }
        curState = REPL_IN_COMPLETION;
        this.replicationJob.completeWithTick(indication.getMessage().getBytes());
        return Answer.ok();
    }

    private Answer replInterrupt() {
        this.clear();
        next = null;
        return Answer.ok();
    }

}
