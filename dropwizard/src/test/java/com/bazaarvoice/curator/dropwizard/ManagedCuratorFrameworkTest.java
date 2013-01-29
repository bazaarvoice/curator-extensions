package com.bazaarvoice.curator.dropwizard;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.imps.CuratorFrameworkState;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ManagedCuratorFrameworkTest {
    @Test(expected = NullPointerException.class)
    public void testNullCuratorFramework() {
        new ManagedCuratorFramework(null);
    }

    @Test
    public void testStartsCuratorOnStart() throws Exception {
        CuratorFramework curator = mock(CuratorFramework.class);
        when(curator.getState()).thenReturn(CuratorFrameworkState.LATENT);

        ManagedCuratorFramework managed = new ManagedCuratorFramework(curator);
        managed.start();

        verify(curator).start();
    }

    @Test
    public void testDoesNotRestartAlreadyStartedCurator() throws Exception {
        CuratorFramework curator = mock(CuratorFramework.class);
        when(curator.getState()).thenReturn(CuratorFrameworkState.STARTED);

        ManagedCuratorFramework managed = new ManagedCuratorFramework(curator);
        managed.start();

        verify(curator, never()).start();
    }

    @Test
    public void testClosesCuratorOnStop() throws Exception {
        CuratorFramework curator = mock(CuratorFramework.class);
        when(curator.getState()).thenReturn(CuratorFrameworkState.STARTED);

        ManagedCuratorFramework managed = new ManagedCuratorFramework(curator);
        managed.start();
        managed.stop();

        verify(curator).close();
    }

    @Test
    public void testDoesNotStopUnstartedCurator() throws Exception {
        CuratorFramework curator = mock(CuratorFramework.class);
        when(curator.getState()).thenReturn(CuratorFrameworkState.LATENT);

        ManagedCuratorFramework managed = new ManagedCuratorFramework(curator);
        managed.start();
        managed.stop();

        verify(curator, never()).close();
    }

    @Test
    public void testDoesNotStopAlreadyStoppedCurator() throws Exception {
        CuratorFramework curator = mock(CuratorFramework.class);
        when(curator.getState()).thenReturn(CuratorFrameworkState.STOPPED);

        ManagedCuratorFramework managed = new ManagedCuratorFramework(curator);
        managed.start();
        managed.stop();

        verify(curator, never()).close();
    }
}
