package cn.shijinshi.redis.control.rpc;

/**
 * OK 具体的意义，视使用详情而定
 * BAD 当发生错误
 *
 * NEXT 一般用于回应SYNC_PING 和 REPL_PING，
 * 表示当前阶段已完成，可以进行一下阶段。
 *
 * EXPIRE 每个Indication都有一个epoch，Broker收到Indication后，
 * 会对比epoch，以此判断Indication是否来自失效的controller。
 *
 * @author Gui Jiahai
 */
public enum State {
    OK, BAD, NEXT, EXPIRE
}
