package com.bazaarvoice.curator.dropwizard;

import com.netflix.curator.framework.CuratorFramework;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CuratorHealthCheckTest {
    @Test(expected = NullPointerException.class)
    public void testNullCurator() {
        new CuratorHealthCheck(null);
    }

    @Test
    public void testConnected() throws Exception {
        CuratorFramework curator = mock(CuratorFramework.class, RETURNS_DEEP_STUBS);
        when(curator.getZookeeperClient().isConnected()).thenReturn(true);

        CuratorHealthCheck healthCheck = new CuratorHealthCheck(curator);
        assertTrue(healthCheck.check().isHealthy());
    }

    @Test
    public void testNotConnected() throws Exception {
        CuratorFramework curator = mock(CuratorFramework.class, RETURNS_DEEP_STUBS);
        when(curator.getZookeeperClient().isConnected()).thenReturn(false);

        CuratorHealthCheck healthCheck = new CuratorHealthCheck(curator);
        assertFalse(healthCheck.check().isHealthy());
    }
}
