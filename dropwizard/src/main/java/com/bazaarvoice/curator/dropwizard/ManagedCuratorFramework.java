package com.bazaarvoice.curator.dropwizard;

import com.google.common.io.Closeables;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.imps.CuratorFrameworkState;
import com.yammer.dropwizard.lifecycle.Managed;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Managed interface for {@code CuratorFramework}.  This will cleanly close the ZooKeeper connection when a Dropwizard
 * application shuts down.
 */
public class ManagedCuratorFramework implements Managed {
    private final CuratorFramework _curator;

    public ManagedCuratorFramework(CuratorFramework curator) {
        _curator = checkNotNull(curator);
    }

    @Override
    public void start() throws Exception {
        if (_curator.getState() == CuratorFrameworkState.LATENT) {
            _curator.start();
        }
    }

    @Override
    public void stop() throws Exception {
        if (_curator.getState() == CuratorFrameworkState.STARTED) {
            Closeables.closeQuietly(_curator);
        }
    }
}
