package com.bazaarvoice.curator.recipes;

import com.bazaarvoice.curator.test.ZooKeeperTest;
import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class NodeDiscoveryTest extends ZooKeeperTest {
    private static final Logger LOG = LoggerFactory.getLogger(NodeDiscoveryTest.class);
    private static final String PATH = "/path";
    private static final String FOO = ZKPaths.makePath(PATH, "foo");

    // A node that's not under the path that the node discovery watches
    private static final String UNWATCHED = ZKPaths.makePath("/other-path", "node");

    private static final NodeDiscovery.NodeDataParser<String> PARSER = new NodeDiscovery.NodeDataParser<String>() {
        @Override
        public String parse(String path, byte[] data) {
            return new String(data);
        }
    };

    private NodeDiscovery<String> _nodeDiscovery;
    private CuratorFramework _curator;

    @Before
    @Override
    public void setup() throws Exception {
        super.setup();

        _nodeDiscovery = newDiscovery(PATH, PARSER);
        _nodeDiscovery.start();

        _curator = newCurator();
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Constructor tests
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    @Test(expected = NullPointerException.class)
    public void testNullConnection() {
        new NodeDiscovery<String>(null, PATH, PARSER);
    }

    @Test(expected = NullPointerException.class)
    public void testNullPath() throws Exception {
        new NodeDiscovery<String>(_curator, null, PARSER);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEmptyPath() throws Exception {
        new NodeDiscovery<String>(_curator, "", PARSER);
    }

    @Test(expected = NullPointerException.class)
    public void testNullParser() throws Exception {
        new NodeDiscovery<String>(_curator, PATH, null);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // getNodes() tests
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    @Test
    public void testGetNodesAfterAddNode() throws Exception {
        createNode(FOO);
        assertTrue(waitUntilSize(_nodeDiscovery.getNodes(), 1));
    }

    @Test
    public void testGetNodesAfterUpdateNode() throws Exception {
        createNode(FOO);
        assertTrue(waitUntilSize(_nodeDiscovery.getNodes(), 1));

        updateNode(FOO, "data".getBytes());
        assertTrue(waitUntilValue(_nodeDiscovery.getNodes(), FOO, "data"));
    }

    @Test
    public void testGetNodesAfterRemoveNode() throws Exception {
        createNode(FOO);
        assertTrue(waitUntilSize(_nodeDiscovery.getNodes(), 1));

        deleteNode(FOO);
        assertTrue(waitUntilSize(_nodeDiscovery.getNodes(), 0));
    }

    @Test
    public void testNoNodesAfterClose() throws Exception {
        createNode(FOO);
        assertTrue(waitUntilSize(_nodeDiscovery.getNodes(), 1));

        _nodeDiscovery.close();

        // After closing, NodeDiscovery should return no nodes so clients won't work if they accidentally keep using it.
        assertTrue(_nodeDiscovery.getNodes().isEmpty());
    }

    @Test
    public void testGetNodesReadyAfterStart() throws Exception {
        createNode(FOO);

        // Create the NodeDiscovery after registration is done so there's at least one initial node
        NodeDiscovery<String> nodeDiscovery = newDiscovery(PATH, PARSER);
        nodeDiscovery.start();

        // It should be visible immediately because start is synchronous (no waitUntilSize call here).
        assertEquals(1, nodeDiscovery.getNodes().size());
    }

    @Test
    public void testGetNodesIgnoresNodesInDifferentPath() throws Exception {
        WatchTrigger created = WatchTrigger.creationTrigger();
        _nodeDiscovery.getCurator().checkExists().usingWatcher(created).forPath(UNWATCHED);

        createNode(UNWATCHED);
        assertTrue(created.firedWithin(10, TimeUnit.SECONDS));
        assertTrue(_nodeDiscovery.getNodes().isEmpty());
    }

    @Test
    public void testGetNodesIgnoresNodeRemovedDuringStart() throws Exception {
        createNode(FOO);
        assertTrue(waitUntilSize(_nodeDiscovery.getNodes(), 1));

        final NodeDiscovery<String> discovery = newDiscovery(PATH, PARSER);

        // Delete FOO and start the discovery as close together as possible, so they race with each other.
        List<Runnable> runnables = Lists.newArrayList(
                new Runnable() {
                    @Override
                    public void run() {
                        try {
                            deleteNode(FOO);
                        } catch (Exception e) {
                            throw Throwables.propagate(e);
                        }
                    }
                },

                new Runnable() {
                    @Override
                    public void run() {
                        discovery.start();
                    }
                }
        );
        Collections.shuffle(runnables);

        for (Runnable runnable : runnables) {
            new Thread(runnable).start();
        }

        assertTrue(waitUntilSize(discovery.getNodes(), 0));
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // contains() tests
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    @Test
    public void testContainsDataWhenAddNode() throws Exception {
        createNode(FOO, "data".getBytes());
        assertTrue(waitUntilSize(_nodeDiscovery.getNodes(), 1));

        assertTrue(_nodeDiscovery.contains("data"));
    }

    @Test
    public void testContainsDataWhenUpdateNode() throws Exception {
        createNode(FOO, "data".getBytes());
        assertTrue(waitUntilSize(_nodeDiscovery.getNodes(), 1));

        updateNode(FOO, "new data".getBytes());
        assertTrue(waitUntilValue(_nodeDiscovery.getNodes(), FOO, "new data"));

        assertFalse(_nodeDiscovery.contains("data"));
        assertTrue(_nodeDiscovery.contains("new data"));
    }

    @Test
    public void testContainsDataWhenRemoveNode() throws Exception {
        createNode(FOO, "data".getBytes());
        assertTrue(waitUntilSize(_nodeDiscovery.getNodes(), 1));

        deleteNode(FOO);
        assertTrue(waitUntilSize(_nodeDiscovery.getNodes(), 0));
        assertFalse(_nodeDiscovery.contains("data"));
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Listener tests
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    @Test
    public void testListenerCalledWhenAddNode() throws Exception {
        AddTrigger<String> trigger = new AddTrigger<String>(FOO);
        _nodeDiscovery.addListener(trigger);

        createNode(FOO);
        assertTrue(trigger.firedWithin(10, TimeUnit.SECONDS));
    }

    @Test
    public void testListenerCalledWhenUpdateNode() throws Exception {
        createNode(FOO);
        assertTrue(waitUntilSize(_nodeDiscovery.getNodes(), 1));

        UpdateTrigger<String> trigger = new UpdateTrigger<String>(FOO);
        _nodeDiscovery.addListener(trigger);

        updateNode(FOO, "updated data".getBytes());
        assertTrue(trigger.firedWithin(10, TimeUnit.SECONDS));
    }

    @Test
    public void testListenerCalledWhenRemoveNode() throws Exception {
        createNode(FOO);
        assertTrue(waitUntilSize(_nodeDiscovery.getNodes(), 1));

        RemoveTrigger<String> trigger = new RemoveTrigger<String>(FOO);
        _nodeDiscovery.addListener(trigger);

        deleteNode(FOO);
        assertTrue(trigger.firedWithin(10, TimeUnit.SECONDS));
    }

    @Test
    public void testListenerCalledDuringStart() throws Exception {
        createNode(FOO);

        NodeDiscovery<String> nodeDiscovery = newDiscovery(PATH, PARSER);

        AddTrigger<String> trigger = new AddTrigger<String>(FOO);
        nodeDiscovery.addListener(trigger);

        nodeDiscovery.start();
        assertEquals(1, nodeDiscovery.getNodes().size());
        assertTrue(trigger.firedWithin(10, TimeUnit.SECONDS));
    }

    @Test
    public void testRemovedListenerNotCalledWhenAddNode() throws Exception {
        AddTrigger<String> trigger = new AddTrigger<String>(FOO);
        _nodeDiscovery.addListener(trigger);
        _nodeDiscovery.removeListener(trigger);

        createNode(FOO);
        assertTrue(waitUntilSize(_nodeDiscovery.getNodes(), 1));
        assertFalse(trigger.hasFired());
    }

    @Test
    public void testRemovedListenerNotCalledWhenUpdateNode() throws Exception {
        createNode(FOO);

        UpdateTrigger<String> trigger = new UpdateTrigger<String>(FOO);
        _nodeDiscovery.addListener(trigger);
        _nodeDiscovery.removeListener(trigger);

        updateNode(FOO, "data".getBytes());
        waitUntilValue(_nodeDiscovery.getNodes(), FOO, "data");
        assertFalse(trigger.hasFired());
    }

    @Test
    public void testRemovedListenerNotCalledWhenRemoveNode() throws Exception {
        createNode(FOO);

        RemoveTrigger<String> trigger = new RemoveTrigger<String>(FOO);
        _nodeDiscovery.addListener(trigger);
        _nodeDiscovery.removeListener(trigger);

        deleteNode(FOO);
        assertTrue(waitUntilSize(_nodeDiscovery.getNodes(), 0));
        assertFalse(trigger.hasFired());
    }

    @Test
    public void testMultipleListenersCalledWhenAddNode() throws Exception {
        AddTrigger<String> trigger1 = new AddTrigger<String>(FOO);
        AddTrigger<String> trigger2 = new AddTrigger<String>(FOO);
        _nodeDiscovery.addListener(trigger1);
        _nodeDiscovery.addListener(trigger2);

        createNode(FOO);
        assertTrue(waitUntilSize(_nodeDiscovery.getNodes(), 1));

        assertTrue(trigger1.firedWithin(10, TimeUnit.SECONDS));
        assertTrue(trigger2.firedWithin(10, TimeUnit.SECONDS));
    }

    @Test
    public void testMultipleListenersCalledWhenUpdateNode() throws Exception {
        createNode(FOO);
        assertTrue(waitUntilSize(_nodeDiscovery.getNodes(), 1));

        UpdateTrigger<String> trigger1 = new UpdateTrigger<String>(FOO);
        UpdateTrigger<String> trigger2 = new UpdateTrigger<String>(FOO);
        _nodeDiscovery.addListener(trigger1);
        _nodeDiscovery.addListener(trigger2);

        updateNode(FOO, "updated data".getBytes());
        assertTrue(trigger1.firedWithin(10, TimeUnit.SECONDS));
        assertTrue(trigger2.firedWithin(10, TimeUnit.SECONDS));
    }

    @Test
    public void testMultipleListenersCalledWhenRemoveNode() throws Exception {
        createNode(FOO);
        assertTrue(waitUntilSize(_nodeDiscovery.getNodes(), 1));

        RemoveTrigger<String> trigger1 = new RemoveTrigger<String>(FOO);
        RemoveTrigger<String> trigger2 = new RemoveTrigger<String>(FOO);
        _nodeDiscovery.addListener(trigger1);
        _nodeDiscovery.addListener(trigger2);

        deleteNode(FOO);
        assertTrue(trigger1.firedWithin(10, TimeUnit.SECONDS));
        assertTrue(trigger2.firedWithin(10, TimeUnit.SECONDS));
    }

    @Test
    public void testListenerNotCalledWhenDifferentPathAddNode() throws Exception {
        AddTrigger<String> trigger = new AddTrigger<String>(UNWATCHED);
        _nodeDiscovery.addListener(trigger);

        WatchTrigger created = WatchTrigger.creationTrigger();
        _nodeDiscovery.getCurator().checkExists().usingWatcher(created).forPath(UNWATCHED);

        createNode(UNWATCHED);
        assertTrue(created.firedWithin(10, TimeUnit.SECONDS));
        assertFalse(trigger.hasFired());
    }

    @Test
    public void testListenerNotCalledWhenDifferentPathUpdateNode() throws Exception {
        UpdateTrigger<String> trigger = new UpdateTrigger<String>(UNWATCHED);
        _nodeDiscovery.addListener(trigger);

        WatchTrigger created = WatchTrigger.creationTrigger();
        _nodeDiscovery.getCurator().checkExists().usingWatcher(created).forPath(UNWATCHED);

        createNode(UNWATCHED);
        assertTrue(created.firedWithin(10, TimeUnit.SECONDS));

        WatchTrigger updated = WatchTrigger.updateTrigger();
        _nodeDiscovery.getCurator().getData().usingWatcher(updated).forPath(UNWATCHED);

        updateNode(UNWATCHED, "updated data".getBytes());
        assertTrue(updated.firedWithin(10, TimeUnit.SECONDS));
        assertFalse(trigger.hasFired());
    }

    @Test
    public void testListenerNotCalledWhenDifferentPathRemoveNode() throws Exception {
        RemoveTrigger<String> trigger = new RemoveTrigger<String>(UNWATCHED);
        _nodeDiscovery.addListener(trigger);

        WatchTrigger created = WatchTrigger.creationTrigger();
        _nodeDiscovery.getCurator().checkExists().usingWatcher(created).forPath(UNWATCHED);

        createNode(UNWATCHED);
        assertTrue(created.firedWithin(10, TimeUnit.SECONDS));

        WatchTrigger deleted = WatchTrigger.deletionTrigger();
        _nodeDiscovery.getCurator().checkExists().usingWatcher(deleted).forPath(UNWATCHED);

        deleteNode(UNWATCHED);
        assertTrue(deleted.firedWithin(10, TimeUnit.SECONDS));
        assertFalse(trigger.hasFired());
    }

    @Test
    public void testListenerCalledWithNodeValueWhenAddNode() throws Exception {
        final AtomicReference<String> actualData = new AtomicReference<String>();
        AddTrigger<String> trigger = new AddTrigger<String>(FOO) {
            @Override
            public void onNodeAdded(String path, String data) {
                actualData.set(data);
                super.onNodeAdded(path, data);
            }
        };
        _nodeDiscovery.addListener(trigger);

        createNode(FOO, "data".getBytes());
        assertTrue(waitUntilSize(_nodeDiscovery.getNodes(), 1));

        assertTrue(trigger.firedWithin(10, TimeUnit.SECONDS));
        assertEquals("data", actualData.get());
    }

    @Test
    public void testListenerCalledWithNodeValueWhenUpdateNode() throws Exception {
        final AtomicReference<String> actualData = new AtomicReference<String>();
        UpdateTrigger<String> trigger = new UpdateTrigger<String>(FOO) {
            @Override
            public void onNodeUpdated(String path, String data) {
                actualData.set(data);
                super.onNodeUpdated(path, data);
            }
        };
        _nodeDiscovery.addListener(trigger);

        createNode(FOO);
        assertTrue(waitUntilSize(_nodeDiscovery.getNodes(), 1));

        updateNode(FOO, "data".getBytes());
        assertTrue(trigger.firedWithin(10, TimeUnit.SECONDS));
        assertEquals("data", actualData.get());
    }

    @Test
    public void testListenerCalledWithNodeValueWhenRemoveNode() throws Exception {
        final AtomicReference<String> actualData = new AtomicReference<String>();
        RemoveTrigger<String> trigger = new RemoveTrigger<String>(FOO) {
            @Override
            public void onNodeRemoved(String path, String data) {
                actualData.set(data);
                super.onNodeRemoved(path, data);
            }
        };
        _nodeDiscovery.addListener(trigger);

        createNode(FOO, "data".getBytes());
        assertTrue(waitUntilSize(_nodeDiscovery.getNodes(), 1));

        deleteNode(FOO);
        assertTrue(trigger.firedWithin(10, TimeUnit.SECONDS));
        assertEquals("data", actualData.get());
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Parser tests
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    @Test
    public void testParserReturnValueUsedWhenAddNode() throws Exception {
        final Object value = new Object();
        NodeDiscovery<Object> discovery = newDiscovery(PATH, new NodeDiscovery.NodeDataParser<Object>() {
            @Override
            public Object parse(String path, byte[] nodeData) {
                return value;
            }
        });
        discovery.start();

        createNode(FOO);
        assertTrue(waitUntilValue(discovery.getNodes(), FOO, value));
    }

    @Test
    public void testParserReturnValueUsedWhenUpdateNode() throws Exception {
        final Object value = new Object();
        NodeDiscovery<Object> discovery = newDiscovery(PATH, new NodeDiscovery.NodeDataParser<Object>() {
            int count = 0;

            @Override
            public Object parse(String path, byte[] nodeData) {
                if (count++ == 0) return new Object();
                return value;
            }
        });
        discovery.start();

        createNode(FOO);
        assertTrue(waitUntilSize(discovery.getNodes(), 1));
        assertNotSame(discovery.getNodes().get(FOO), value);

        updateNode(FOO, "data".getBytes());
        assertTrue(waitUntilValue(discovery.getNodes(), FOO, value));
    }

    @Test
    public void testParserReturnValueOfNullAllowed() throws Exception {
        NodeDiscovery<Object> discovery = newDiscovery(PATH, new NodeDiscovery.NodeDataParser<Object>() {
            @Override
            public Object parse(String path, byte[] nodeData) {
                return null;
            }
        });
        discovery.start();

        createNode(FOO, "data".getBytes());
        assertTrue(waitUntilSize(discovery.getNodes(), 1));
        assertNull(discovery.getNodes().get(FOO));
    }

    @Test
    public void testParserExceptionTreatedAsNull() throws Exception {
        NodeDiscovery<Object> discovery = newDiscovery(PATH, new NodeDiscovery.NodeDataParser<Object>() {
            @Override
            public Object parse(String path, byte[] nodeData) {
                throw new RuntimeException();
            }
        });
        discovery.start();

        createNode(FOO, "data".getBytes());
        assertTrue(waitUntilSize(discovery.getNodes(), 1));
        assertNull(discovery.getNodes().get(FOO));
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Connection tests
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    @Test
    public void testWithoutZooKeeper() throws Exception {
        stopZooKeeper();

        // Trying to start a node discovery instance while there's no zookeeper server running should succeed without
        // throwing any exceptions.
        NodeDiscovery<String> discovery = newDiscovery(PATH, PARSER);
        discovery.start();
        discovery.close();
    }

    @Test
    public void testRemembersNodesWhenZooKeeperIsStopped() throws Exception {
        createNode(FOO);
        assertTrue(waitUntilSize(_nodeDiscovery.getNodes(), 1));

        stopZooKeeper();
        assertTrue(waitUntilSize(_nodeDiscovery.getNodes(), 1));
    }

    @Test
    public void testRemembersNodesWhenSessionIsLost() throws Exception {
        createNode(FOO);
        assertTrue(waitUntilSize(_nodeDiscovery.getNodes(), 1));

        killSession(_nodeDiscovery.getCurator());
        assertTrue(waitUntilSize(_nodeDiscovery.getNodes(), 1));
    }

    @Test
    public void testWithZooKeeperRestart() throws Exception {
        createNode(FOO);
        assertTrue(waitUntilSize(_nodeDiscovery.getNodes(), 1));

        ConnectionTrigger reconnected = ConnectionTrigger.reconnectedTrigger();
        _curator.getConnectionStateListenable().addListener(reconnected);
        restartZooKeeper();

        assertTrue(reconnected.firedWithin(10, TimeUnit.SECONDS));
        assertTrue(waitUntilSize(_nodeDiscovery.getNodes(), 1));
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Close tests
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    /** Verify that starting and stopping a NodeDiscovery object doesn't leak threads. */
    @Test
    public void testThreadLeak() throws Exception {
        CuratorFramework curator = newCurator();

        final Set<Thread> threadsAtStart = Thread.getAllStackTraces().keySet();

        NodeDiscovery<String> nodeDiscovery = closer().register(new NodeDiscovery<String>(curator, PATH, PARSER));
        nodeDiscovery.start();
        nodeDiscovery.close();

        // Get the threads at the end.  Since NodeDiscovery calls shutdown() but doesn't awaitTermination() we may
        // have to wait a little while for the NodeDiscovery test to terminate.  Wait up to 10 seconds for things
        // to settle before failing the test (in the success case the loop will terminate quickly).
        assertTrue(waitUntil(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                Set<Thread> threadsAtEnd = Thread.getAllStackTraces().keySet();
                Set<Thread> difference = Sets.difference(threadsAtEnd, threadsAtStart);
                LOG.info("Extra threads: {}", difference);
                return difference.isEmpty();
            }
        }));
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Helper functions
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    private <T> NodeDiscovery<T> newDiscovery(String path, NodeDiscovery.NodeDataParser<T> parser) throws Exception {
        return closer().register(new NodeDiscovery<T>(newCurator(), path, parser));
    }

    /** Create a node. */
    private void createNode(String path) throws Exception {
        createNode(path, new byte[0]);
    }

    /** Create a node with specific data. */
    private void createNode(String path, byte[] data) throws Exception {
        _curator.create().creatingParentsIfNeeded().forPath(path, data);
    }

    /** Update the data contained within a node. */
    private void updateNode(String path, byte[] newData) throws Exception {
        _curator.setData().forPath(path, newData);
    }

    /** Delete a node. */
    private void deleteNode(String path) throws Exception {
        _curator.delete().forPath(path);
    }

    private static boolean waitUntil(Callable<Boolean> function) throws Exception {
        long start = System.nanoTime();
        while (System.nanoTime() - start <= TimeUnit.SECONDS.toNanos((long) 10)) {
            if (function.call()) {
                return true;
            }

            Thread.yield();
        }

        return false;
    }

    private static <K, T> boolean waitUntilSize(Map<K, T> map, int size) {
        long start = System.nanoTime();
        while (System.nanoTime() - start <= TimeUnit.SECONDS.toNanos((long) 10)) {
            if (map.size() == size) {
                return true;
            }

            Thread.yield();
        }

        return false;
    }

    private static <K, T> boolean waitUntilValue(Map<K, T> map, K key, T value) {
        long start = System.nanoTime();
        while (System.nanoTime() - start <= TimeUnit.SECONDS.toNanos((long) 10)) {
            if (Objects.equal(map.get(key), value)) {
                return true;
            }

            Thread.yield();
        }

        return false;
    }

    private static class AbstractTrigger<T> extends Trigger implements NodeDiscovery.NodeListener<T> {
        @Override
        public void onNodeAdded(String path, T node) {
        }

        @Override
        public void onNodeRemoved(String path, T node) {
        }

        @Override
        public void onNodeUpdated(String path, T node) {
        }
    }

    private static class AddTrigger<T> extends AbstractTrigger<T> {
        private final String _path;

        public AddTrigger(String path) {
            _path = path;
        }

        @Override
        public void onNodeAdded(String path, T node) {
            if (_path.equals(path)) {
                fire();
            }
        }
    }

    private static class RemoveTrigger<T> extends AbstractTrigger<T> {
        private final String _path;

        public RemoveTrigger(String path) {
            _path = path;
        }

        @Override
        public void onNodeRemoved(String path, T node) {
            if (_path.equals(path)) {
                fire();
            }
        }
    }

    private static class UpdateTrigger<T> extends AbstractTrigger<T> {
        private final String _path;

        public UpdateTrigger(String path) {
            _path = path;
        }

        @Override
        public void onNodeUpdated(String path, T node) {
            if (_path.equals(path)) {
                fire();
            }
        }
    }
}
