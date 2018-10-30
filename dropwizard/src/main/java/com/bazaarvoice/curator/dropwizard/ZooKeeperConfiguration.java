package com.bazaarvoice.curator.dropwizard;

import com.bazaarvoice.curator.ResolvingEnsembleProvider;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.dropwizard.lifecycle.setup.LifecycleEnvironment;
import io.dropwizard.setup.Environment;
import io.dropwizard.util.Duration;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.hibernate.validator.valuehandling.UnwrapValidatedValue;

import javax.validation.constraints.NotNull;
import java.util.Optional;
import java.util.concurrent.ThreadFactory;

/** Jackson friendly object for holding configuration information about a ZooKeeper ensemble. */
public class ZooKeeperConfiguration {
    private static final String DEFAULT_CONNECT_STRING = "localhost:2181";
    private static final RetryPolicy DEFAULT_RETRY_POLICY = new BoundedExponentialBackoffRetry(100, 1000, 5);

    @NotNull
    @JsonProperty("connectString")
    @UnwrapValidatedValue(false)
    private Optional<String> _connectString = Optional.empty();

    @JsonProperty("namespace")
    private Optional<String> _namespace = Optional.empty();

    @JsonProperty("retryPolicy")
    private RetryPolicy _configRetryPolicy = null;

    @JsonProperty("sessionTimeout")
    private Duration _sessionTimeout = Duration.seconds(60);

    @JsonProperty("connectionTimeout")
    private Duration _connectionTimeout = Duration.seconds(15);

    /**
     * Used to hold a retry policy provided by a setter.  This needs to be separate from {@code _retryPolicy} because
     * we want callers to be able to specify any Curator {@link org.apache.curator.RetryPolicy} implementation instead
     * of the inner {@link RetryPolicy} and its subclasses that are used entirely to hold Jackson annotations.
     */
    private Optional<org.apache.curator.RetryPolicy> _setterRetryPolicy = Optional.empty();

    /**
     * Return a new Curator connection to the ensemble.  It is the caller's responsibility to start and close the
     * connection.
     */
    public CuratorFramework newCurator() {
        // Make all of the curator threads daemon threads so they don't block the JVM from terminating.  Also label them
        // with the ensemble they're connecting to, in case someone is trying to sort through a thread dump.
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("CuratorFramework[" + _connectString.orElse(DEFAULT_CONNECT_STRING) + "]-%d")
                .setDaemon(true)
                .build();

        org.apache.curator.RetryPolicy retry = _setterRetryPolicy.orElse(
                (_configRetryPolicy != null)
                        ? _configRetryPolicy
                        : DEFAULT_RETRY_POLICY
        );
        return CuratorFrameworkFactory.builder()
                .ensembleProvider(new ResolvingEnsembleProvider(_connectString.orElse(DEFAULT_CONNECT_STRING)))
                .retryPolicy(retry)
                .sessionTimeoutMs(Ints.checkedCast(_sessionTimeout.toMilliseconds()))
                .connectionTimeoutMs(Ints.checkedCast(_connectionTimeout.toMilliseconds()))
                .namespace(_namespace.orElse(null))
                .threadFactory(threadFactory)
                .build();
    }

    /**
     * Return a managed Curator connection.  This created connection will be wrapped in a
     * {@link ManagedCuratorFramework} and offered to the provided {@link Environment} parameter.
     *
     * @deprecated Use {@link #newManagedCurator(LifecycleEnvironment)} instead.
     */
    @Deprecated
    public CuratorFramework newManagedCurator(Environment env) {
        return newManagedCurator(env.lifecycle());
    }

    /**
     * Return a managed Curator connection.  This created connection will be wrapped in a
     * {@link ManagedCuratorFramework} and offered to the provided {@link LifecycleEnvironment} parameter.
     */
    public CuratorFramework newManagedCurator(LifecycleEnvironment env) {
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
    public Optional<org.apache.curator.RetryPolicy> getRetryPolicy() {
        if (_setterRetryPolicy.isPresent()) {
            return _setterRetryPolicy;
        }

        return Optional.<org.apache.curator.RetryPolicy>ofNullable(_configRetryPolicy);
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
    public void setRetryPolicy(org.apache.curator.RetryPolicy retryPolicy) {
        _setterRetryPolicy = Optional.of(retryPolicy);
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
    @JsonSubTypes({
            @JsonSubTypes.Type(value = BoundedExponentialBackoffRetry.class, name = "boundedExponentialBackoff"),
            @JsonSubTypes.Type(value = ExponentialBackoffRetry.class, name = "exponentialBackoff"),
            @JsonSubTypes.Type(value = RetryNTimes.class, name = "nTimes"),
            @JsonSubTypes.Type(value = RetryUntilElapsed.class, name = "untilElapsed")
    })
    static interface RetryPolicy extends org.apache.curator.RetryPolicy {
    }

    private static final class BoundedExponentialBackoffRetry
            extends org.apache.curator.retry.BoundedExponentialBackoffRetry
            implements RetryPolicy {
        @JsonCreator
        public BoundedExponentialBackoffRetry(@JsonProperty("baseSleepTimeMs") int baseSleepTimeMs,
                                              @JsonProperty("maxSleepTimeMs") int maxSleepTimeMs,
                                              @JsonProperty("maxRetries") int maxRetries) {
            super(baseSleepTimeMs, maxSleepTimeMs, maxRetries);
        }
    }

    private static final class ExponentialBackoffRetry
            extends org.apache.curator.retry.ExponentialBackoffRetry
            implements RetryPolicy {
        @JsonCreator
        public ExponentialBackoffRetry(@JsonProperty("baseSleepTimeMs") int baseSleepTimeMs,
                                       @JsonProperty("maxRetries") int maxRetries) {
            super(baseSleepTimeMs, maxRetries);
        }
    }

    private static final class RetryNTimes
            extends org.apache.curator.retry.RetryNTimes
            implements RetryPolicy {
        @JsonCreator
        public RetryNTimes(@JsonProperty("n") int n,
                           @JsonProperty("sleepMsBetweenRetries") int sleepMsBetweenRetries) {
            super(n, sleepMsBetweenRetries);
        }
    }

    private static final class RetryUntilElapsed
            extends org.apache.curator.retry.RetryUntilElapsed
            implements RetryPolicy {
        public RetryUntilElapsed(@JsonProperty("maxElapsedTimeMs") int maxElapsedTimeMs,
                                 @JsonProperty("sleepMsBetweenRetries") int sleepMsBetweenRetries) {
            super(maxElapsedTimeMs, sleepMsBetweenRetries);
        }
    }
}
