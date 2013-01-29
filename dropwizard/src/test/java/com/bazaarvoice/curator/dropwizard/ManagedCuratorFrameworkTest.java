package com.bazaarvoice.curator.dropwizard;

import com.netflix.curator.framework.CuratorFramework;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class ManagedCuratorFrameworkTest {
    @Test(expected = NullPointerException.class)
    public void testNullCuratorFramework() {
        new ManagedCuratorFramework(null);
    }

    @Test
    public void testClosesOnStop() throws Exception {
        CuratorFramework curator = mock(CuratorFramework.class);

        ManagedCuratorFramework managed = new ManagedCuratorFramework(curator);
        managed.start();
        managed.stop();

        verify(curator).close();
    }
}
