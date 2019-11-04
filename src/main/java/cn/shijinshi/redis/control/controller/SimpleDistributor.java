package cn.shijinshi.redis.control.controller;

import cn.shijinshi.redis.common.param.Cluster;
import cn.shijinshi.redis.common.param.HostAndPort;
import cn.shijinshi.redis.common.param.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * 简单的分配器
 *
 * @author Gui Jiahai
 */
public class SimpleDistributor implements Distributor {

    private static final Logger logger = LoggerFactory.getLogger(SimpleDistributor.class);

    @Override
    public Cluster distribute(Cluster redisCluster, Cluster oldBrokerCluster, Set<HostAndPort> brokerSet) {
        if (brokerSet.size() < redisCluster.getNodes().size()) {
            logger.warn(">>> The number of brokers must not be less than the number of redis nodes");
            return null;
        }
        return balance(redisCluster, oldBrokerCluster, brokerSet);
    }

    private Cluster balance(Cluster redisCluster, Cluster oldBrokerCluster, Set<HostAndPort> brokerSet) {
        brokerSet = new HashSet<>(brokerSet);
        List<Node> newNodes = new ArrayList<>();

        Map<String, Integer> counter = new HashMap<>();
        for (Node redisNode : redisCluster.getNodes()) {
            Node newNode = new Node();
            newNodes.add(newNode);

            HostAndPort redis = redisNode.getRedis();
            newNode.setRedis(redis);
            newNode.setRanges(new ArrayList<>(redisNode.getRanges()));

            HostAndPort oldBroker = getOldBroker(redis, oldBrokerCluster);
            if (oldBroker != null && brokerSet.contains(oldBroker)) {
                newNode.setBroker(oldBroker);
                brokerSet.remove(oldBroker);

                Integer c = counter.getOrDefault(oldBroker.getHost(), 0);
                counter.put(oldBroker.getHost(), c + 1);
            }
        }

        Map<String, List<HostAndPort>> group = group(brokerSet);
        for (String host : group.keySet()) {
            counter.putIfAbsent(host, 0);
        }

        for (Node newNode : newNodes) {
            if (newNode.getBroker() == null) {
                while (true) {
                    String leastUsedHost = getLeastUsedHost(counter);
                    if (leastUsedHost == null) {
                        return null;
                    }

                    List<HostAndPort> brokers = group.get(leastUsedHost);
                    if (brokers == null || brokers.isEmpty()) {
                        counter.put(leastUsedHost, Integer.MAX_VALUE);
                        continue;
                    }
                    HostAndPort selected = brokers.remove(0);
                    newNode.setBroker(selected);
                    brokerSet.remove(selected);
                    if (brokers.isEmpty()) {
                        group.remove(leastUsedHost);
                    }
                    counter.put(leastUsedHost, counter.get(leastUsedHost) + 1);
                    break;
                }
            }
        }


        Iterator<HostAndPort> iterator = brokerSet.iterator();
        int num = (brokerSet.size() + (newNodes.size() - 1)) / newNodes.size();
        for (Node newNode : newNodes) {
            List<HostAndPort> slaves = new ArrayList<>(num);
            newNode.setSlaves(slaves);
            for (int i = 0; i < num; i++) {
                if (!iterator.hasNext()) {
                    break;
                }
                slaves.add(iterator.next());
            }
        }

        return new Cluster(newNodes);
    }

    private String getLeastUsedHost(Map<String, Integer> map) {
        String host = null;
        int count = Integer.MAX_VALUE;

        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            int eCount = entry.getValue();
            if (eCount == 0) {
                return entry.getKey();
            } else if (eCount < count) {
                count = eCount;
                host = entry.getKey();
            }
        }
        return count == Integer.MAX_VALUE ? null : host;
    }

    private HostAndPort getOldBroker(HostAndPort redis, Cluster oldBrokerCluster) {
        if (oldBrokerCluster != null) {
            for (Node node : oldBrokerCluster.getNodes()) {
                if (node.getRedis().equals(redis)) {
                    return node.getBroker();
                }
            }
        }
        return null;
    }

    /**
     * 比较理想的情况是，所有的压力均摊在所有机器上，
     * 所以，这里将Broker按照机器地址进行归类
     */
    private Map<String, List<HostAndPort>> group(Set<HostAndPort> brokerSet) {
        Map<String, List<HostAndPort>> map = new HashMap<>();
        for (HostAndPort broker : brokerSet) {
            List<HostAndPort> list = map.computeIfAbsent(broker.getHost(), s -> new LinkedList<>());
            list.add(broker);
        }

        for (List<HostAndPort> list : map.values()) {
            list.sort(Comparator.comparingInt(HostAndPort::getPort));
        }

        return map;
    }


}
