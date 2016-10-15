package ru.shishmakov.ch;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.LoadBalancer;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.util.AbstractLoadBalancer;
import com.hazelcast.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static ru.shishmakov.hz.cfg.HzClientConfig.buildClientConfig;
import static ru.shishmakov.hz.cfg.HzClientConfig.buildHZClientInstance;
import static ru.shishmakov.hz.cfg.HzClusterConfig.buildHZInstance;

/**
 * @author Dmitriy Shishmakov
 */
public class Chapter8_HazelcastClients {
    private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final Integer POISON_PILL = -1;

    public static void doExamples(HazelcastInstance hz1, HazelcastInstance hz2, ExecutorService service) {
        logger.debug("-- Chapter 8. Hazelcast Clients --");

        try {
//        clientConnectToCluster(hz1, hz2);
//        clientConnectionListener(hz1, hz2);
            clientLoadBalancer(hz1, hz2);
        } finally {
            HazelcastClient.shutdownAll();
        }
    }

    private static void clientLoadBalancer(HazelcastInstance hz1, HazelcastInstance hz2) {
        logger.debug("-- HZ LoadBalancer --");

        HazelcastInstance hz3 = buildHZInstance();

        ClientConfig config = buildClientConfig();
        config.setLoadBalancer(new ClientLoadBalancer());
        HazelcastInstance hzClient = HazelcastClient.newHazelcastClient(config);

        IMap<String, String> map1 = hzClient.getMap("map1");
        logger.debug("Balancer should find first cluster member");
        map1.set("key1", "value1");
        map1.set("key2", "value2");
        IMap<String, String> map2 = hzClient.getMap("map2");
        logger.debug("Balancer should find second cluster member");
        map2.set("key3", "value3");
        map2.set("key4", "value4");

        hz3.shutdown();
        logger.debug("Balancer should decrease count of cluster members");
        map1.destroy();
        map2.destroy();
    }

    private static void clientConnectionListener(HazelcastInstance hz1, HazelcastInstance hz2) {
        logger.debug("-- HZ Connection Listener --");

        String listenerId1 = hz1.getClientService().addClientListener(new HzClientListener());
        String listenerId2 = hz2.getClientService().addClientListener(new HzClientListener());
        logger.debug("add client listener 1: {}", listenerId1);
        logger.debug("add client listener 2: {}", listenerId2);
        HazelcastInstance hzClient = buildHZClientInstance();

        for (int threshold = 100; threshold > 0; threshold--) {
            Collection<Client> clients = hz1.getClientService().getConnectedClients();
            logger.debug("client quantities: {}", clients.size());

            if (hzClient.getLifecycleService().isRunning()) break;
            if (threshold == 1) logger.warn("cluster doesn't have client!");
        }

    }

    private static void clientConnectToCluster(HazelcastInstance hz1, HazelcastInstance hz2) {
        logger.debug("-- HZ Client use Cluster --");

        HazelcastInstance hzClient = buildHZClientInstance();
        IQueue<Integer> queueNode2 = hz2.getQueue("queue");
        try {
            for (int i = 0; i < 10; i++) {
                queueNode2.put(i);
                logger.debug("queueNode2 put --> {}", i);
            }
            queueNode2.put(POISON_PILL);
            logger.debug("queueNode2 put --> POISON_PILL={}; last item", POISON_PILL);
        } catch (InterruptedException e) {
            logger.debug("Error", e);
        }

        IQueue<Integer> queueClient = hzClient.getQueue("queue");
        try {
            for (int threshold = 100; threshold > 0; threshold--) {
                int item = queueClient.poll(500, TimeUnit.MILLISECONDS);
                if (POISON_PILL.equals(item)) {
                    logger.debug("queueClient poll x-- POISON_PILL={}; exit", item);
                    break;
                }
                logger.debug("queueClient poll <-- {}", item);
            }
            logger.debug("queueClient end iteration");
        } catch (InterruptedException e) {
            logger.debug("Error", e);
        }

        queueNode2.destroy();
    }

    public static class HzClientListener implements ClientListener, Serializable {
        private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

        @Override
        public void clientConnected(Client client) {
            logger.debug("client id: {} is Connected", client.getUuid());
        }

        @Override
        public void clientDisconnected(Client client) {
            logger.debug("client id: {} is Disconnected", client.getUuid());
        }
    }

    /**
     * See hz implementation {@link AbstractLoadBalancer}
     */
    public static class ClientLoadBalancer implements LoadBalancer, InitialMembershipListener, Serializable {
        private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

        private final AtomicReference<Member[]> membersRef = new AtomicReference<>(new Member[]{});
        private volatile Cluster clusterRef;

        @Override
        public final void init(Cluster cluster, ClientConfig config) {
            logger.debug("init load balancer!");
            this.clusterRef = cluster;
            cluster.addMembershipListener(this);
        }

        @Override
        public Member next() {
            Member[] members = membersRef.get();
            if (members == null || members.length == 0) {
                logger.debug("the cluster members not found");
                return null;
            }

            ThreadLocalRandom random = ThreadLocalRandom.current();
            logger.debug("members count: {}", members.length);
            Member member = members[random.nextInt(0, members.length)];
            logger.debug("choose next member id: {}", member.getUuid());
            return member;
        }

        @Override
        public final void init(InitialMembershipEvent event) {
            logger.debug("register load balancer!");
            setMembersRef();
        }

        @Override
        public final void memberAdded(MembershipEvent event) {
            Member member = event.getMember();
            logger.debug("member was added; id: {}, address: {}, ", member.getUuid(), member.getAddress());
            setMembersRef();
        }

        @Override
        public final void memberRemoved(MembershipEvent event) {
            Member member = event.getMember();
            logger.debug("member was removed; id: {}, address: {}, ", member.getUuid(), member.getAddress());
            setMembersRef();
        }

        @Override
        public final void memberAttributeChanged(MemberAttributeEvent event) {
        }

        private void setMembersRef() {
            Set<Member> memberSet = clusterRef.getMembers();
            Member[] members = memberSet.stream().toArray(Member[]::new);
            membersRef.set(members);
        }
    }
}
