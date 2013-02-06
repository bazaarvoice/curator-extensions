package com.bazaarvoice.curator.dropwizard;

import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.imps.CuratorFrameworkState;
import com.netflix.curator.retry.BoundedExponentialBackoffRetry;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import com.netflix.curator.retry.RetryNTimes;
import com.netflix.curator.retry.RetryUntilElapsed;
import com.yammer.dropwizard.config.Environment;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class ZooKeeperConfigurationTest {
    private final ObjectMapper _parser = new MappingJsonFactory().getCodec();

    @Test
    public void testDeserializeConnectString() throws IOException {
        ZooKeeperConfiguration config = parse(ImmutableMap.of("connect-string", "host:port"));
        assertEquals("host:port", config.getConnectString());
    }

    @Test
    public void testDefaultConnectString() throws IOException {
        ZooKeeperConfiguration config = parse(ImmutableMap.<String, Object>of());
        assertEquals("localhost:2181", config.getConnectString());
    }

    @Test
    public void testDeserializeNamespace() throws IOException {
        ZooKeeperConfiguration config = parse(ImmutableMap.of("namespace", "/ns"));
        assertEquals("/ns", config.getNamespace());
    }

    @Test
    public void testDefaultNamespace() throws IOException {
        ZooKeeperConfiguration config = parse(ImmutableMap.<String, Object>of());
        assertNull(config.getNamespace());
    }

    @Test
    public void testDefaultRetry() throws IOException {
        ZooKeeperConfiguration config = parse(ImmutableMap.<String, Object>of());
        assertNotNull(config.getRetryPolicy());
    }

    @Test
    public void testDeserializeBoundedExponentialBackoffRetry() throws IOException {
        ZooKeeperConfiguration config = parse(ImmutableMap.of("retry-policy",
                ImmutableMap.builder()
                        .put("type", "bounded-exponential-backoff")
                        .put("base-sleep-time-ms", 50)
                        .put("max-sleep-time-ms", 500)
                        .put("max-retries", 3)
                        .build()));
        assertTrue(config.getRetryPolicy() instanceof BoundedExponentialBackoffRetry);
    }

    @Test
    public void testDeserializeExponentialBackoffRetry() throws IOException {
        ZooKeeperConfiguration config = parse(ImmutableMap.of("retry-policy",
                ImmutableMap.builder()
                        .put("type", "exponential-backoff")
                        .put("base-sleep-time-ms", 50)
                        .put("max-retries", 3)
                        .build()));
        assertTrue(config.getRetryPolicy() instanceof ExponentialBackoffRetry);
    }

    @Test
    public void testDeserializeRetryNTimes() throws IOException {
        ZooKeeperConfiguration config = parse(ImmutableMap.of("retry-policy",
                ImmutableMap.builder()
                        .put("type", "n-times")
                        .put("n", 1)
                        .put("sleep-ms-between-retries", 50)
                        .build()));
        assertTrue(config.getRetryPolicy() instanceof RetryNTimes);
    }

    @Test
    public void testDeserializeRetryUntilElapsed() throws IOException {
        ZooKeeperConfiguration config = parse(ImmutableMap.of("retry-policy",
                ImmutableMap.builder()
                        .put("type", "until-elapsed")
                        .put("max-elapsed-time-ms", 1000)
                        .put("sleep-ms-between-retries", 50)
                        .build()));
        assertTrue(config.getRetryPolicy() instanceof RetryUntilElapsed);
    }

    @Test
    public void testNewCurator() throws IOException {
        ZooKeeperConfiguration config = parse(ImmutableMap.of("retry-policy",
                ImmutableMap.builder()
                        .put("type", "until-elapsed")
                        .put("max-elapsed-time-ms", 1000)
                        .put("sleep-ms-between-retries", 50)
                        .build()));
        CuratorFramework curator = config.newCurator();

        assertNotNull(curator);
        assertEquals(CuratorFrameworkState.LATENT, curator.getState());
    }

    @Test
    public void testNewManagedCurator() throws IOException {
        ZooKeeperConfiguration config = parse(ImmutableMap.of("retry-policy",
                ImmutableMap.builder()
                        .put("type", "until-elapsed")
                        .put("max-elapsed-time-ms", 1000)
                        .put("sleep-ms-between-retries", 50)
                        .build()));

        Environment env = mock(Environment.class);
        CuratorFramework curator = config.newManagedCurator(env);

        assertNotNull(curator);
        assertEquals(CuratorFrameworkState.LATENT, curator.getState());
        verify(env).manage(any(ManagedCuratorFramework.class));
    }

    private ZooKeeperConfiguration parse(Map<String, ?> map) throws IOException {
        String json = _parser.writeValueAsString(map);
        return _parser.readValue(json, ZooKeeperConfiguration.class);
    }
}
