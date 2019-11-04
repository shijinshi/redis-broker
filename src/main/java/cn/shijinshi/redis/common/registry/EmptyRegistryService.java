package cn.shijinshi.redis.common.registry;

import java.util.Collections;
import java.util.List;

/**
 * 模拟的注册服务。
 * 当程序以特殊模式运行时，可能并不需要连接注册中心。
 *
 * @author Gui Jiahai
 */
public class EmptyRegistryService implements RegistryService {

    @Override
    public String create(String path, boolean ephemeral, boolean sequential) {
        return path;
    }

    @Override
    public void delete(String path) {

    }

    @Override
    public List<String> getChildren(String path) {
        return Collections.emptyList();
    }

    @Override
    public void addChildListener(String path, ChildListener listener) {

    }

    @Override
    public void removeChildListener(String path, ChildListener listener) {

    }

    @Override
    public byte[] getData(String path) {
        return new byte[0];
    }

    @Override
    public void setData(String path, byte[] data) {

    }

    @Override
    public void addDataListener(String path, DataListener listener) {

    }

    @Override
    public void removeDataListener(String path, DataListener listener) {

    }

    @Override
    public void addStateListener(StateListener listener) {

    }

    @Override
    public void removeStateListener(StateListener listener) {

    }

    @Override
    public boolean checkExists(String path) {
        return false;
    }

    @Override
    public boolean isConnected() {
        return true;
    }

    @Override
    public void close() {

    }
}
