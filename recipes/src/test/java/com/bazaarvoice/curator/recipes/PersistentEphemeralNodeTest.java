package com.bazaarvoice.curator.recipes;

import com.bazaarvoice.curator.test.ZooKeeperTest;
import com.google.common.collect.Lists;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Test;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class PersistentEphemeralNodeTest extends ZooKeeperTest {
    private static final String DIR = "/test";
    private static final String PATH = ZKPaths.makePath(DIR, "/foo");
    private static final byte[] DATA = "data".getBytes();

    /** This curator instance is used to verify all interaction with ZooKeeper from an external user's perspective. */
    private CuratorFramework _curator;

    /** Keep track of the nodes that were created during this test so that they can be cleaned up at the end. */
    private final Collection<PersistentEphemeralNode> _createdNodes = Lists.newArrayList();

    @Override
    public void setup() throws Exception {
        super.setup();
        _curator = newCurator();
    }

    @After
    @Override
    public void teardown() throws Exception {
        for (PersistentEphemeralNode node : _createdNodes) {
            node.close(10, TimeUnit.SECONDS);
        }

        super.teardown();
    }

    @Test(expected = NullPointerException.class)
    public void testNullCurator() throws Exception {
        new PersistentEphemeralNode(null, PATH, DATA, CreateMode.EPHEMERAL);
    }

    @Test(expected = NullPointerException.class)
    public void testNullPath() throws Exception {
        new PersistentEphemeralNode(newCurator(), null, DATA, CreateMode.EPHEMERAL);
    }

    @Test(expected = NullPointerException.class)
    public void testNullData() throws Exception {
        new PersistentEphemeralNode(newCurator(), PATH, null, CreateMode.EPHEMERAL);
    }

    @Test(expected = NullPointerException.class)
    public void testNullMode() throws Exception {
        new PersistentEphemeralNode(newCurator(), PATH, DATA, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNonPersistentMode() throws Exception {
        new PersistentEphemeralNode(newCurator(), PATH, DATA, CreateMode.PERSISTENT);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNonPersistentSequentialMode() throws Exception {
        new PersistentEphemeralNode(newCurator(), PATH, DATA, CreateMode.PERSISTENT_SEQUENTIAL);
    }

    @Test
    public void testCreatesNodeOnConstruction() throws Exception {
        PersistentEphemeralNode node = createNode(PATH);
        assertNodeExists(_curator, node.getActualPath());
    }

    @Test
    public void testDeletesNodeWhenClosed() throws Exception {
        PersistentEphemeralNode node = createNode(PATH);
        assertNodeExists(_curator, node.getActualPath());

        String path = node.getActualPath();
        node.close(10, TimeUnit.SECONDS);  // After closing the path is set to null...
        assertNodeDoesNotExist(_curator, path);
    }

    @Test
    public void testClosingMultipleTimes() throws Exception {
        PersistentEphemeralNode node = createNode(PATH);
        assertNodeExists(_curator, node.getActualPath());

        String path = node.getActualPath();
        node.close(10, TimeUnit.SECONDS);
        assertNodeDoesNotExist(_curator, path);

        node.close(10, TimeUnit.SECONDS);
        assertNodeDoesNotExist(_curator, path);
    }

    @Test
    public void testDeletesNodeWhenSessionDisconnects() throws Exception {
        PersistentEphemeralNode node = createNode(PATH);
        assertNodeExists(_curator, node.getActualPath());

        // Register a watch that will fire when the node is deleted...
        WatchTrigger deletedWatchTrigger = WatchTrigger.deletionTrigger();
        _curator.checkExists().usingWatcher(deletedWatchTrigger).forPath(node.getActualPath());

        killSession(node.getCurator());

        // Make sure the node got deleted
        assertTrue(deletedWatchTrigger.firedWithin(10, TimeUnit.SECONDS));
    }

    @Test
    public void testRecreatesNodeWhenSessionReconnects() throws Exception {
        PersistentEphemeralNode node = createNode(PATH);
        assertNodeExists(_curator, node.getActualPath());

        WatchTrigger deletedWatchTrigger = WatchTrigger.deletionTrigger();
        _curator.checkExists().usingWatcher(deletedWatchTrigger).forPath(node.getActualPath());

        killSession(node.getCurator());

        // Make sure the node got deleted...
        assertTrue(deletedWatchTrigger.firedWithin(10, TimeUnit.SECONDS));

        // Check for it to be recreated...
        WatchTrigger createdWatchTrigger = WatchTrigger.creationTrigger();
        Stat stat = _curator.checkExists().usingWatcher(createdWatchTrigger).forPath(node.getActualPath());
        assertTrue(stat != null || createdWatchTrigger.firedWithin(10, TimeUnit.SECONDS));
    }

    @Test
    public void testRecreatesNodeWhenSessionReconnectsMultipleTimes() throws Exception {
        PersistentEphemeralNode node = createNode(PATH);
        String path = node.getActualPath();
        assertNodeExists(_curator, path);

        // We should be able to disconnect multiple times and each time the node should be recreated.
        for (int i = 0; i < 5; i++) {
            WatchTrigger deletionTrigger = WatchTrigger.deletionTrigger();
            _curator.checkExists().usingWatcher(deletionTrigger).forPath(path);

            // Kill the session, thus cleaning up the node...
            killSession(node.getCurator());

            // Make sure the node ended up getting deleted...
            assertTrue(deletionTrigger.firedWithin(10, TimeUnit.SECONDS));

            // Now put a watch in the background looking to see if it gets created...
            WatchTrigger creationTrigger = WatchTrigger.creationTrigger();
            Stat stat = _curator.checkExists().usingWatcher(creationTrigger).forPath(path);
            assertTrue(stat != null || creationTrigger.firedWithin(10, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testRecreatesNodeWhenItGetsDeleted() throws Exception {
        PersistentEphemeralNode node = createNode(PATH, CreateMode.EPHEMERAL);
        String originalNode = node.getActualPath();
        assertNodeExists(_curator, originalNode);

        // Delete the original node (from an external zookeeper)...
        _curator.delete().forPath(originalNode);

        // Since we're using an ephemeral node, and the original session hasn't been interrupted the name of the new
        // node that gets created is going to be exactly the same as the original.
        WatchTrigger createdWatchTrigger = WatchTrigger.creationTrigger();
        Stat stat = _curator.checkExists().usingWatcher(createdWatchTrigger).forPath(originalNode);
        assertTrue(stat != null || createdWatchTrigger.firedWithin(10, TimeUnit.SECONDS));
    }

    @Test
    public void testNodesCreateUniquePaths() throws Exception {
        PersistentEphemeralNode node1 = createNode(PATH, CreateMode.EPHEMERAL);
        String path1 = node1.getActualPath();

        PersistentEphemeralNode node2 = createNode(PATH, CreateMode.EPHEMERAL);
        String path2 = node2.getActualPath();

        assertFalse(path1.equals(path2));
    }

    @Test
    public void testData() throws Exception {
        PersistentEphemeralNode node = createNode(PATH);
        byte[] bytes = _curator.getData().forPath(node.getActualPath());

        assertArrayEquals(bytes, DATA);
    }

    private PersistentEphemeralNode createNode(String path) throws Exception {
        return createNode(path, CreateMode.EPHEMERAL);
    }

    private PersistentEphemeralNode createNode(String path, CreateMode mode) throws Exception {
        PersistentEphemeralNode node = new PersistentEphemeralNode(newCurator(), path, DATA, mode);
        _createdNodes.add(node);
        return node;
    }

    private void assertNodeExists(CuratorFramework curator, String path) throws Exception {
        assertNotNull(path);
        assertTrue(curator.checkExists().forPath(path) != null);
    }

    private void assertNodeDoesNotExist(CuratorFramework curator, String path) throws Exception {
        assertTrue(curator.checkExists().forPath(path) == null);
    }
}
