package com.bazaarvoice.curator.dropwizard;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.yammer.dropwizard.config.Environment;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.NotNull;
import java.util.concurrent.ThreadFactory;

/** Jackson friendly object for holding configuration information about a ZooKeeper ensemble. */
public class ZooKeeperConfiguration {
    @NotNull
    @NotEmpty
    @JsonProperty("connect-string")
    private String _connectString = "localhost:2181";

    @JsonProperty("namespace")
    private String _namespace = null;

    @NotNull
    @JsonProperty("retry-policy")
    private RetryPolicy _retryPolicy = new BoundedExponentialBackoffRetry(100, 1000, 5);

    /**
     * Return a new Curator connection to the ensemble.  It is the caller's responsibility to start and close the
     * connection.
     */
    public CuratorFramework newCurator() {
        // Make all of the curator threads daemon threads so they don't block the JVM from terminating.  Also label them
        // with the ensemble they're connecting to, in case someone is trying to sort through a thread dump.
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("CuratorFramework[" + _connectString + "]-%d")
                .setDaemon(true)
                .build();

        return CuratorFrameworkFactory.builder()
                .connectString(_connectString)
                .retryPolicy(_retryPolicy)
                .namespace(_namespace)
                .threadFactory(threadFactory)
                .build();
    }

    /**
     * Return a managed Curator connection.  This created connection will be wrapped in a
     * {@link ManagedCuratorFramework} and offered to the provided {@link Environment} parameter.
     */
    public CuratorFramework newManagedCurator(Environment env) {
        CuratorFramework curator = newCurator();
        env.manage(new ManagedCuratorFramework(curator));
        return curator;
    }

    @VisibleForTesting
    String getConnectString() {
        return _connectString;
    }

    @VisibleForTesting
    String getNamespace() {
        return _namespace;
    }

    @VisibleForTesting
    RetryPolicy getRetryPolicy() {
        return _retryPolicy;
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
    @JsonSubTypes({
            @JsonSubTypes.Type(value = BoundedExponentialBackoffRetry.class, name = "bounded-exponential-backoff"),
            @JsonSubTypes.Type(value = ExponentialBackoffRetry.class, name = "exponential-backoff"),
            @JsonSubTypes.Type(value = RetryNTimes.class, name = "n-times"),
            @JsonSubTypes.Type(value = RetryUntilElapsed.class, name = "until-elapsed")
    })
    @VisibleForTesting
    static interface RetryPolicy extends com.netflix.curator.RetryPolicy {
    }

    private static final class BoundedExponentialBackoffRetry
            extends com.netflix.curator.retry.BoundedExponentialBackoffRetry
            implements RetryPolicy {
        @JsonCreator
        public BoundedExponentialBackoffRetry(@JsonProperty("base-sleep-time-ms") int baseSleepTimeMs,
                                              @JsonProperty("max-sleep-time-ms") int maxSleepTimeMs,
                                              @JsonProperty("max-retries") int maxRetries) {
            super(baseSleepTimeMs, maxSleepTimeMs, maxRetries);
        }
    }

    private static final class ExponentialBackoffRetry
            extends com.netflix.curator.retry.ExponentialBackoffRetry
            implements RetryPolicy {
        @JsonCreator
        public ExponentialBackoffRetry(@JsonProperty("base-sleep-time-ms") int baseSleepTimeMs,
                                       @JsonProperty("max-retries") int maxRetries) {
            super(baseSleepTimeMs, maxRetries);
        }
    }

    private static final class RetryNTimes
            extends com.netflix.curator.retry.RetryNTimes
            implements RetryPolicy {
        @JsonCreator
        public RetryNTimes(@JsonProperty("n") int n,
                           @JsonProperty("sleep-ms-between-retries") int sleepMsBetweenRetries) {
            super(n, sleepMsBetweenRetries);
        }
    }

    private static final class RetryUntilElapsed
            extends com.netflix.curator.retry.RetryUntilElapsed
            implements RetryPolicy {
        public RetryUntilElapsed(@JsonProperty("max-elapsed-time-ms") int maxElapsedTimeMs,
                                 @JsonProperty("sleep-ms-between-retries") int sleepMsBetweenRetries) {
            super(maxElapsedTimeMs, sleepMsBetweenRetries);
        }
    }
}
