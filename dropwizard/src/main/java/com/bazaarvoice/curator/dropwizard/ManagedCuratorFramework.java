package com.bazaarvoice.curator.dropwizard;

import com.google.common.io.Closeables;
import io.dropwizard.lifecycle.Managed;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;

import java.util.Objects;


/**
 * Managed interface for {@code CuratorFramework}.  This will cleanly close the ZooKeeper connection when a Dropwizard
 * application shuts down.
 */
public class ManagedCuratorFramework implements Managed {
    private final CuratorFramework _curator;

    public ManagedCuratorFramework(CuratorFramework curator) {
        _curator = Objects.requireNonNull(curator);
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
            Closeables.close(_curator, true);
        }
    }
}
