package com.bazaarvoice.zookeeper;

import com.bazaarvoice.zookeeper.internal.CuratorConnection;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.netflix.curator.RetryPolicy;
import com.netflix.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.zookeeper.common.PathUtils;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * ZooKeeper connection configuration class.
 * <p/>
 * NOTE: If you modify this class then you <b>must</b> also modify the corresponding class in the soa-dropwizard
 * artifact.  If you don't then you've just exposed configuration options that can't be easily used by people writing
 * dropwizard services.
 */
public class ZooKeeperConfiguration {
    private String _connectString = null;
    private RetryPolicy _retryPolicy = new BoundedExponentialBackoffRetry(100, 1000, 5);
    private String _namespace;

    /**
     * Returns a new {@link ZooKeeperConnection} with the current configuration settings.
     *
     * @return A new {@link ZooKeeperConnection} with the current configuration settings.
     */
    public ZooKeeperConnection connect() {
        checkNotNull(_connectString);
        return new CuratorConnection(_connectString, _retryPolicy, _namespace);
    }

    /**
     * Sets a ZooKeeper connection string that looks like "host:port,host:port,...".  The
     * connection string must list at least one live member of the ZooKeeper ensemble, and
     * should list all members of the ZooKeeper ensemble in case any one member is temporarily
     * unavailable.
     *
     * @param connectString A ZooKeeper connection string.
     */
    public ZooKeeperConfiguration withConnectString(String connectString) {
        _connectString = checkNotNull(connectString);
        return this;
    }

    /** Sets a retry policy that retries up to a set number of times with an exponential backoff between retries. */
    public ZooKeeperConfiguration withBoundedExponentialBackoffRetry(int initialSleepTimeMs, int maxSleepTimeMs,
                                                                     int maxNumAttempts) {
        checkArgument(maxNumAttempts > 0);
        checkArgument(initialSleepTimeMs > 0);
        checkArgument(maxSleepTimeMs > 0);

        // The Curator retry policies take as a parameter the number of times a retry is allowed.  So we convert
        // maxNumAttempts into maxNumRetries.
        _retryPolicy = new BoundedExponentialBackoffRetry(initialSleepTimeMs, maxSleepTimeMs, maxNumAttempts - 1);
        return this;
    }

    /**
     * Sets a namespace that will be prefixed to every path used by the ZooKeeperConnection.
     * Typically the namespace will be "/global" or the name of the local data center.  If non-empty, the namespace
     * must be a valid ZooKeeper path (starts with '/', does not end with '/', etc).
     */
    public ZooKeeperConfiguration withNamespace(String namespace) {
        if (!Strings.isNullOrEmpty(namespace)) {
            PathUtils.validatePath(namespace);
        }
        _namespace = namespace;
        return this;
    }

    @VisibleForTesting
    protected String getConnectString() {
        return _connectString;
    }

    @VisibleForTesting
    protected RetryPolicy getRetryPolicy() {
        return _retryPolicy;
    }

    @VisibleForTesting
    protected String getNamespace() {
        return _namespace;
    }
}