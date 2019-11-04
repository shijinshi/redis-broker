package cn.shijinshi.redis.common.registry;

import java.util.List;

/**
 * 注册服务。
 * 在分布式系统中，用于各个节点之间注册和发现，
 * 使各个节点能够互相协调工作。
 *
 * 用于注册中心大多都使用zookeeper实现的，
 * 所以基本可以认为注册中心就是zookeeper。
 *
 * @author Gui Jiahai
 */
public interface RegistryService {

    /**
     * 在注册中心创建节点。
     *
     * @param path 要创建节点的path
     * @param ephemeral 是否是临时节点
     * @param sequential 是否带序号
     * @return 返回创建成功后的节点路径，如果有序号，则路径带有序号。
     */
    String create(String path, boolean ephemeral, boolean sequential) throws RegistryException;

    /**
     * 删除节点
     */
    void delete(String path) throws RegistryException;

    /**
     * 获取当前节点的子节点
     */
    List<String> getChildren(String path) throws RegistryException;

    /**
     * 在节点上添加子节点监听器。
     * 当子节点发生变化时，会通知listener
     */
    void addChildListener(String path, ChildListener listener);

    /**
     * 删除节点上的节点监听器
     */
    void removeChildListener(String path, ChildListener listener);

    /**
     * 获取节点上存储的数据
     */
    byte[] getData(String path) throws RegistryException;

    /**
     * 向节点上写入数据
     */
    void setData(String path, byte[] data) throws RegistryException;

    /**
     * 在节点上添加数据监听器
     * 当数据发生变化时，会通知listener
     */
    void addDataListener(String path, DataListener listener);

    /**
     * 删除节点上的数据监听器
     */
    void removeDataListener(String path, DataListener listener);

    /**
     * 添加状态监听器
     */
    void addStateListener(StateListener listener);

    /**
     * 删除状态监听器
     */
    void removeStateListener(StateListener listener);

    /**
     * 检查节点是否存在
     */
    boolean checkExists(String path) throws RegistryException;

    /**
     * 检查是否与注册中心保持连接
     */
    boolean isConnected();

    void close();

}
