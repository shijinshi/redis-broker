package cn.shijinshi.redis.common.registry;

import java.util.List;

/**
 * 用于监听节点下的子节点。
 *
 * @author Gui Jiahai
 */
public interface ChildListener {

    /**
     * 当子节点发生变化时，会收到全量的子节点列表。
     */
    void childChanged(String path, List<String> children);

}
