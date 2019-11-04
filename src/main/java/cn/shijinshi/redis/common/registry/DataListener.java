package cn.shijinshi.redis.common.registry;

/**
 * 用于监听节点下的数据
 *
 * @author Gui Jiahai
 */
public interface DataListener {

    void dataChanged(String path, byte[] data);
}
