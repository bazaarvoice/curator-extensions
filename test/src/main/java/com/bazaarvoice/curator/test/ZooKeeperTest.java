package com.bazaarvoice.curator.test;

import com.google.common.base.Throwables;
import com.google.common.io.Closeables;
import com.google.common.io.Closer;
import org.apache.curator.CuratorZookeeperClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.KillSession;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertNotNull;

public abstract class ZooKeeperTest {
    /** The ZooKeeper server that all created CuratorFramework instances will communicate with. */
    private TestingServer _zooKeeperServer;

    /**
     * The instance spec for the ZooKeeper testing server.  We want to hold onto a single instance of this for the life
     * of a test case because the instance spec contains the pork that the ZooKeeper testing server listens on.  If we
     * end up restarting the server for some reason, we want it to re-bind to the same port when it comes up again.
     */
    private final InstanceSpec _instanceSpec = InstanceSpec.newInstanceSpec();

    /** All of the resources that we've created running the test (so they can be cleaned up later). */
    private final Closer _closer = Closer.create();

    @Before
    public void setup() throws Exception {
        startZooKeeper();
    }

    @After
    public void teardown() throws Exception {
        Closeables.close(_closer, true);
    }

    public Closer closer() {
        return _closer;
    }

    public void startZooKeeper() throws Exception {
        _zooKeeperServer = _closer.register(new TestingServer(_instanceSpec, true));
    }

    public void stopZooKeeper() throws IOException {
        _zooKeeperServer.stop();
    }

    @SuppressWarnings("UnusedDeclaration")
    public void restartZooKeeper() throws Exception {
        stopZooKeeper();
        startZooKeeper();
    }

    @SuppressWarnings("UnusedDeclaration")
    public CuratorFramework newCurator() throws Exception {
        return newCurator(CuratorFrameworkFactory.builder().retryPolicy(new RetryNTimes(2, 10)));
    }

    public CuratorFramework newCurator(CuratorFrameworkFactory.Builder builder) throws Exception {
        assertNotNull("ZooKeeper testing server is null, did you forget to call super.setup()", _zooKeeperServer);

        CuratorFramework curator = _closer.register(builder.connectString(_instanceSpec.getConnectString()).build());
        curator.start();

        return curator;
    }

    @SuppressWarnings("UnusedDeclaration")
    public void killSession(CuratorFramework curator) throws Exception {
        CuratorZookeeperClient client = curator.getZookeeperClient();
        KillSession.kill(client.getZooKeeper(), client.getCurrentConnectionString());
    }

    @SuppressWarnings("UnusedDeclaration")
    public static class Trigger {
        private final CountDownLatch _latch;

        public Trigger() {
            _latch = new CountDownLatch(1);
        }

        public void fire() {
            _latch.countDown();
        }

        public boolean firedWithin(long duration, TimeUnit unit) {
            try {
                return _latch.await(duration, unit);
            } catch (InterruptedException e) {
                throw Throwables.propagate(e);
            }
        }

        public boolean hasFired() {
            return _latch.getCount() == 0;
        }
    }

    @SuppressWarnings("UnusedDeclaration")
    public static class WatchTrigger extends Trigger implements Watcher {
        private final Event.EventType _expected;

        public static WatchTrigger creationTrigger() {
            return new WatchTrigger(Event.EventType.NodeCreated);
        }

        public static WatchTrigger updateTrigger() {
            return new WatchTrigger(Event.EventType.NodeDataChanged);
        }

        public static WatchTrigger childrenTrigger() {
            return new WatchTrigger(Event.EventType.NodeChildrenChanged);
        }

        public static WatchTrigger deletionTrigger() {
            return new WatchTrigger(Event.EventType.NodeDeleted);
        }

        WatchTrigger(Event.EventType expected) {
            _expected = expected;
        }

        @Override
        public void process(WatchedEvent event) {
            if (_expected.equals(event.getType())) {
                fire();
            }
        }
    }

    @SuppressWarnings("UnusedDeclaration")
    public static class ConnectionTrigger extends Trigger implements ConnectionStateListener {
        private final ConnectionState _expected;

        public static ConnectionTrigger lostTrigger() {
            return new ConnectionTrigger(ConnectionState.LOST);
        }

        public static ConnectionTrigger reconnectedTrigger() {
            return new ConnectionTrigger(ConnectionState.RECONNECTED);
        }

        ConnectionTrigger(ConnectionState expected) {
            _expected = expected;
        }

        @Override
        public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
            if (_expected.equals(connectionState)) {
                fire();
            }
        }
    }
}
