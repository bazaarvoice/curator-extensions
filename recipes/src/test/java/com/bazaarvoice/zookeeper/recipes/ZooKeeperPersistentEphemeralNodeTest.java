package com.bazaarvoice.zookeeper.recipes;

import com.bazaarvoice.zookeeper.ZooKeeperConnection;
import com.bazaarvoice.zookeeper.internal.CuratorConnection;
import com.bazaarvoice.zookeeper.test.ZooKeeperTest;
import com.google.common.collect.Lists;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Test;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ZooKeeperPersistentEphemeralNodeTest extends ZooKeeperTest {
    private static final String DIR = "/test";
    private static final String PATH = ZKPaths.makePath(DIR, "/foo");
    private static final byte[] DATA = "data".getBytes();

    private Collection<ZooKeeperPersistentEphemeralNode> _createdNodes = Lists.newArrayList();

    @After
    public void teardown() throws Exception {
        for (ZooKeeperPersistentEphemeralNode node : _createdNodes) {
            node.close(10, TimeUnit.SECONDS);
        }

        super.teardown();
    }

    @Test(expected = NullPointerException.class)
    public void testNullCurator() throws Exception {
        new ZooKeeperPersistentEphemeralNode(null, PATH, DATA, CreateMode.EPHEMERAL);
    }

    @Test(expected = NullPointerException.class)
    public void testNullPath() throws Exception {
        ZooKeeperConnection connection = newMockZooKeeperConnection();
        new ZooKeeperPersistentEphemeralNode(connection, null, DATA, CreateMode.EPHEMERAL);
    }

    @Test(expected = NullPointerException.class)
    public void testNullData() throws Exception {
        ZooKeeperConnection connection = newMockZooKeeperConnection();
        new ZooKeeperPersistentEphemeralNode(connection, PATH, null, CreateMode.EPHEMERAL);
    }

    @Test(expected = NullPointerException.class)
    public void testNullMode() throws Exception {
        ZooKeeperConnection connection = newMockZooKeeperConnection();
        new ZooKeeperPersistentEphemeralNode(connection, PATH, DATA, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNonPersistentMode() throws Exception {
        CuratorFramework curator = mock(CuratorFramework.class);
        CuratorConnection connection = mock(CuratorConnection.class);
        when(connection.getCurator()).thenReturn(curator);
        new ZooKeeperPersistentEphemeralNode(connection, PATH, DATA, CreateMode.PERSISTENT);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNonPersistentSequentialMode() throws Exception {
        CuratorFramework curator = mock(CuratorFramework.class);
        CuratorConnection connection = mock(CuratorConnection.class);
        when(connection.getCurator()).thenReturn(curator);
        new ZooKeeperPersistentEphemeralNode(connection, PATH, DATA, CreateMode.PERSISTENT_SEQUENTIAL);
    }

    @Test
    public void testCreatesNodeOnConstruction() throws Exception {
        CuratorFramework curator = newCurator();

        ZooKeeperPersistentEphemeralNode node = createNode(PATH);
        assertNodeExists(curator, node.getActualPath().get());
    }

    @Test
    public void testDeletesNodeWhenClosed() throws Exception {
        CuratorFramework curator = newCurator();

        ZooKeeperPersistentEphemeralNode node = createNode(PATH);
        assertNodeExists(curator, node.getActualPath().get());

        String path = node.getActualPath().get();
        node.close(10, TimeUnit.SECONDS);  // After closing the path is set to null...
        assertNodeDoesNotExist(curator, path);
    }

    @Test
    public void testClosingMultipleTimes() throws Exception {
        CuratorFramework curator = newCurator();
        ZooKeeperPersistentEphemeralNode node = createNode(PATH);

        String path = node.getActualPath().get();
        node.close(10, TimeUnit.SECONDS);
        assertNodeDoesNotExist(curator, path);

        node.close(10, TimeUnit.SECONDS);
        assertNodeDoesNotExist(curator, path);
    }

    @Test
    public void testDeletesNodeWhenSessionDisconnects() throws Exception {
        CuratorFramework curator = newCurator();

        ZooKeeperPersistentEphemeralNode node = createNode(PATH);
        assertNodeExists(curator, node.getActualPath().get());

        // Register a watch that will fire when the node is deleted...
        WatchTrigger deletedWatchTrigger = new WatchTrigger();
        curator.checkExists().usingWatcher(deletedWatchTrigger).forPath(node.getActualPath().get());

        killSession(node.getCurator());

        // Make sure the node got deleted
        assertTrue(deletedWatchTrigger.firedWithin(10, TimeUnit.SECONDS));
    }

    @Test
    public void testRecreatesNodeWhenSessionReconnects() throws Exception {
        CuratorFramework curator = newCurator();

        ZooKeeperPersistentEphemeralNode node = createNode(PATH);
        assertNodeExists(curator, node.getActualPath().get());

        WatchTrigger deletedWatchTrigger = new WatchTrigger();
        curator.checkExists().usingWatcher(deletedWatchTrigger).forPath(node.getActualPath().get());

        killSession(node.getCurator());

        // Make sure the node got deleted...
        assertTrue(deletedWatchTrigger.firedWithin(10, TimeUnit.SECONDS));

        // Check for it to be recreated...
        WatchTrigger createdWatchTrigger = new WatchTrigger();
        Stat stat = curator.checkExists().usingWatcher(createdWatchTrigger).forPath(node.getActualPath().get());
        assertTrue(stat != null || createdWatchTrigger.firedWithin(10, TimeUnit.SECONDS));
    }

    @Test
    public void testRecreatesNodeWhenItGetsDeleted() throws Exception {
        CuratorFramework curator = newCurator();

        ZooKeeperPersistentEphemeralNode node = createNode(PATH, CreateMode.EPHEMERAL);
        String originalNode = node.getActualPath().get();
        assertNodeExists(curator, originalNode);

        // Delete the original node...
        curator.delete().forPath(originalNode);

        // Since we're using an ephemeral node, and the original session hasn't been interrupted the name of the new
        // node that gets created is going to be exactly the same as the original.
        WatchTrigger createdWatchTrigger = new WatchTrigger();
        Stat stat = curator.checkExists().usingWatcher(createdWatchTrigger).forPath(originalNode);
        assertTrue(stat != null || createdWatchTrigger.firedWithin(10, TimeUnit.SECONDS));
    }

    @Test
    public void testNodesCreateUniquePaths() throws Exception {
        ZooKeeperPersistentEphemeralNode node1 = createNode(PATH, CreateMode.EPHEMERAL);
        String path1 = node1.getActualPath().get();

        ZooKeeperPersistentEphemeralNode node2 = createNode(PATH, CreateMode.EPHEMERAL);
        String path2 = node2.getActualPath().get();

        assertFalse(path1.equals(path2));
    }

    private ZooKeeperPersistentEphemeralNode createNode(String path) throws Exception {
        return createNode(path, CreateMode.EPHEMERAL);
    }

    private ZooKeeperPersistentEphemeralNode createNode(String path, CreateMode mode) throws Exception {
        ZooKeeperPersistentEphemeralNode node = new ZooKeeperPersistentEphemeralNode(
                newMockZooKeeperConnection(newCurator()), path, DATA, mode);
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
