package com.bazaarvoice.curator.dropwizard;

import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.retry.RetryUntilElapsed;
import com.yammer.dropwizard.config.Environment;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class ZooKeeperConfigurationTest {
    private final ObjectMapper _parser = new MappingJsonFactory().getCodec().registerModule(new GuavaModule());

    @Test
    public void testMissingConnectString() {
        ZooKeeperConfiguration config = new ZooKeeperConfiguration();
        assertFalse(config.getConnectString().isPresent());
    }

    @Test
    public void testMissingNamespace() {
        ZooKeeperConfiguration config = new ZooKeeperConfiguration();
        assertFalse(config.getNamespace().isPresent());
    }

    @Test
    public void testMissingRetryPolicy() {
        ZooKeeperConfiguration config = new ZooKeeperConfiguration();
        assertFalse(config.getRetryPolicy().isPresent());
    }

    @Test
    public void testSetConnectString() {
        ZooKeeperConfiguration config = new ZooKeeperConfiguration();
        config.setConnectString("host:port");

        assertEquals("host:port", config.getConnectString().get());
    }

    @Test
    public void testSetNamespace() {
        ZooKeeperConfiguration config = new ZooKeeperConfiguration();
        config.setNamespace("/ns");

        assertEquals("/ns", config.getNamespace().get());
    }

    @Test
    public void testSetRetryPolicy() {
        RetryPolicy retry = new RetryOneTime(1);

        ZooKeeperConfiguration config = new ZooKeeperConfiguration();
        config.setRetryPolicy(retry);

        assertSame(retry, config.getRetryPolicy().get());
    }

    @Test
    public void testDeserializeConnectString() {
        ZooKeeperConfiguration config = parse(ImmutableMap.of("connectString", "host:port"));
        assertEquals("host:port", config.getConnectString().get());
    }

    @Test
    public void testDeserializeNamespace() {
        ZooKeeperConfiguration config = parse(ImmutableMap.of("namespace", "/ns"));
        assertEquals("/ns", config.getNamespace().get());
    }

    @Test
    public void testDeserializeBoundedExponentialBackoffRetry() {
        ZooKeeperConfiguration config = parse(ImmutableMap.of("retryPolicy",
                ImmutableMap.builder()
                        .put("type", "boundedExponentialBackoff")
                        .put("baseSleepTimeMs", 50)
                        .put("maxSleepTimeMs", 500)
                        .put("maxRetries", 3)
                        .build()));
        assertTrue(config.getRetryPolicy().get() instanceof BoundedExponentialBackoffRetry);
    }

    @Test
    public void testDeserializeExponentialBackoffRetry() {
        ZooKeeperConfiguration config = parse(ImmutableMap.of("retryPolicy",
                ImmutableMap.builder()
                        .put("type", "exponentialBackoff")
                        .put("baseSleepTimeMs", 50)
                        .put("maxRetries", 3)
                        .build()));
        assertTrue(config.getRetryPolicy().get() instanceof ExponentialBackoffRetry);
    }

    @Test
    public void testDeserializeRetryNTimes() {
        ZooKeeperConfiguration config = parse(ImmutableMap.of("retryPolicy",
                ImmutableMap.builder()
                        .put("type", "nTimes")
                        .put("n", 1)
                        .put("sleepMsBetweenRetries", 50)
                        .build()));
        assertTrue(config.getRetryPolicy().get() instanceof RetryNTimes);
    }

    @Test
    public void testDeserializeRetryUntilElapsed() {
        ZooKeeperConfiguration config = parse(ImmutableMap.of("retryPolicy",
                ImmutableMap.builder()
                        .put("type", "untilElapsed")
                        .put("maxElapsedTimeMs", 1000)
                        .put("sleepMsBetweenRetries", 50)
                        .build()));
        assertTrue(config.getRetryPolicy().get() instanceof RetryUntilElapsed);
    }

    @Test
    public void testNewCurator() {
        ZooKeeperConfiguration config = parse(ImmutableMap.of("retryPolicy",
                ImmutableMap.builder()
                        .put("type", "untilElapsed")
                        .put("maxElapsedTimeMs", 1000)
                        .put("sleepMsBetweenRetries", 50)
                        .build()));
        CuratorFramework curator = config.newCurator();

        assertNotNull(curator);
        assertEquals(CuratorFrameworkState.LATENT, curator.getState());
    }

    @Test
    public void testNewManagedCurator() {
        ZooKeeperConfiguration config = parse(ImmutableMap.of("retryPolicy",
                ImmutableMap.builder()
                        .put("type", "untilElapsed")
                        .put("maxElapsedTimeMs", 1000)
                        .put("sleepMsBetweenRetries", 50)
                        .build()));

        Environment env = mock(Environment.class);
        CuratorFramework curator = config.newManagedCurator(env);

        assertNotNull(curator);
        assertEquals(CuratorFrameworkState.LATENT, curator.getState());
        verify(env).manage(any(ManagedCuratorFramework.class));
    }

    private ZooKeeperConfiguration parse(Map<String, ?> map) {
        try {
            String json = _parser.writeValueAsString(map);
            return _parser.readValue(json, ZooKeeperConfiguration.class);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }
}
