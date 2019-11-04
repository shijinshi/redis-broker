package cn.shijinshi.redis.control.broker;

import cn.shijinshi.redis.common.param.HostAndPort;

/**
 * 在分布式中，每个Broker都有可能会负责一个Redis节点。
 * 当Broker数量多于Redis的Master节点数量时，Broker将不会负责Redis节点。
 *
 * 那么当Broker获知自己负责的Redis节点时，就会把Redis节点的地址通知给相关组件。
 * 所以，如果某个组件需要接收这个通知，则应该实现这个接口，然后注册到Broker上去。
 *
 * @author Gui Jiahai
 */
public interface NodeListener {

    void nodeChanged(HostAndPort address);

}
