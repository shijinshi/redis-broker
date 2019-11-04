package cn.shijinshi.redis.common.prop;

/**
 * @author Gui Jiahai
 */
public class ZookeeperProperties {

    private String address;

    private String root = "/redis-broker";

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getRoot() {
        return root;
    }

    public void setRoot(String root) {
        this.root = root;
    }
}
