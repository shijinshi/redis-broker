package cn.shijinshi.redis.forward.support;

/**
 * @author Gui Jiahai
 */
public class Support {

    private final boolean backup;

    /**
     * 部分命令交本地处理即可，不需要传送给redis处理
     * preparedAction处理之后，会返回Boolean，
     * 如果为true，则表示处理成功，处理结果将会通过future传递，
     * 如果为false，表示preparedAction无法处理，需要交由redis进一步处理
     */
    private final Action preparedAction;

    public Support(boolean backup, Action preparedAction) {
        this.backup = backup;
        this.preparedAction = preparedAction;
    }

    public boolean isBackup() {
        return backup;
    }

    public Action getPreparedAction() {
        return preparedAction;
    }
}
