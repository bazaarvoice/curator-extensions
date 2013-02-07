package com.bazaarvoice.curator.dropwizard;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.yammer.dropwizard.config.Environment;

import javax.validation.constraints.NotNull;
import java.util.concurrent.ThreadFactory;

/** Jackson friendly object for holding configuration information about a ZooKeeper ensemble. */
public class ZooKeeperConfiguration {
    private static final String DEFAULT_CONNECT_STRING = "localhost:2181";
    private static final RetryPolicy DEFAULT_RETRY_POLICY = new BoundedExponentialBackoffRetry(100, 1000, 5);

    @NotNull
    @JsonProperty("connectString")
    private Optional<String> _connectString = Optional.absent();

    @JsonProperty("namespace")
    private Optional<String> _namespace = Optional.absent();

    @NotNull
    @JsonProperty("retryPolicy")
    private Optional<RetryPolicy> _configRetryPolicy = Optional.absent();

    /**
     * Used to hold a retry policy provided by a setter.  This needs to be separate from {@code _retryPolicy} because
     * we want callers to be able to specify any Curator {@link com.netflix.curator.RetryPolicy} implementation instead
     * of the inner {@link RetryPolicy} and its subclasses that are used entirely to hold Jackson annotations.
     */
    private Optional<com.netflix.curator.RetryPolicy> _setterRetryPolicy = Optional.absent();

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
                .connectString(_connectString.or(DEFAULT_CONNECT_STRING))
                .retryPolicy(_setterRetryPolicy.or(_configRetryPolicy.or(DEFAULT_RETRY_POLICY)))
                .namespace(_namespace.orNull())
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

    @JsonIgnore
    public Optional<String> getConnectString() {
        return _connectString;
    }

    @JsonIgnore
    public Optional<String> getNamespace() {
        return _namespace;
    }

    @JsonIgnore
    public Optional<com.netflix.curator.RetryPolicy> getRetryPolicy() {
        if (_setterRetryPolicy.isPresent()) {
            return _setterRetryPolicy;
        }

        return Optional.<com.netflix.curator.RetryPolicy>fromNullable(_configRetryPolicy.orNull());
    }

    @JsonIgnore
    public void setConnectString(String connectString) {
        _connectString = Optional.of(connectString);
    }

    @JsonIgnore
    public void setNamespace(String namespace) {
        _namespace = Optional.of(namespace);
    }

    @JsonIgnore
    public void setRetryPolicy(com.netflix.curator.RetryPolicy retryPolicy) {
        _setterRetryPolicy = Optional.of(retryPolicy);
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
    @JsonSubTypes({
            @JsonSubTypes.Type(value = BoundedExponentialBackoffRetry.class, name = "boundedExponentialBackoff"),
            @JsonSubTypes.Type(value = ExponentialBackoffRetry.class, name = "exponentialBackoff"),
            @JsonSubTypes.Type(value = RetryNTimes.class, name = "nTimes"),
            @JsonSubTypes.Type(value = RetryUntilElapsed.class, name = "untilElapsed")
    })
    static interface RetryPolicy extends com.netflix.curator.RetryPolicy {
    }

    private static final class BoundedExponentialBackoffRetry
            extends com.netflix.curator.retry.BoundedExponentialBackoffRetry
            implements RetryPolicy {
        @JsonCreator
        public BoundedExponentialBackoffRetry(@JsonProperty("baseSleepTimeMs") int baseSleepTimeMs,
                                              @JsonProperty("maxSleepTimeMs") int maxSleepTimeMs,
                                              @JsonProperty("maxRetries") int maxRetries) {
            super(baseSleepTimeMs, maxSleepTimeMs, maxRetries);
        }
    }

    private static final class ExponentialBackoffRetry
            extends com.netflix.curator.retry.ExponentialBackoffRetry
            implements RetryPolicy {
        @JsonCreator
        public ExponentialBackoffRetry(@JsonProperty("baseSleepTimeMs") int baseSleepTimeMs,
                                       @JsonProperty("maxRetries") int maxRetries) {
            super(baseSleepTimeMs, maxRetries);
        }
    }

    private static final class RetryNTimes
            extends com.netflix.curator.retry.RetryNTimes
            implements RetryPolicy {
        @JsonCreator
        public RetryNTimes(@JsonProperty("n") int n,
                           @JsonProperty("sleepMsBetweenRetries") int sleepMsBetweenRetries) {
            super(n, sleepMsBetweenRetries);
        }
    }

    private static final class RetryUntilElapsed
            extends com.netflix.curator.retry.RetryUntilElapsed
            implements RetryPolicy {
        public RetryUntilElapsed(@JsonProperty("maxElapsedTimeMs") int maxElapsedTimeMs,
                                 @JsonProperty("sleepMsBetweenRetries") int sleepMsBetweenRetries) {
            super(maxElapsedTimeMs, sleepMsBetweenRetries);
        }
    }
}
