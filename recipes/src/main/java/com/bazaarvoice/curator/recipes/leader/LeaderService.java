package com.bazaarvoice.curator.recipes.leader;

import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Starts and stops a Guava service based on whether this process is elected leader using a Curator leadership
 * election algorithm.
 * <p>
 * Because Guava services are not restartable, the <code>LeaderService</code> requires a <code>Supplier<Service></code>
 * that it will call each time leadership is acquired.  The <code>LeaderService</code> will ensure that only one
 * such <code>Service</code> instance is running at a time.
 * </p>
 * <p>
 * A typical use of <code>LeaderService</code> for a task that polls an external server might look like this:
 * <pre>
 * CuratorFramework curator = ...;
 * String serverId = new HostAndPort(InetAddress.getLocalHost().getHostAddress(), "8080").toString();
 * final Duration pollInterval = ...;
 *
 * new LeaderService(curator, "/applications/my-app/leader", serverId, 1, TimeUnit.MINUTES, new Supplier<Service>() {
 *     &#64;Override
 *     public Service get() {
 *         return new AbstractScheduledService() {
 *             &#64;Override
 *             protected void runOneIteration() throws Exception {
 *                 poll(); // TODO: implement
 *             }
 *
 *             &#64;Override
 *             protected Scheduler scheduler() {
 *                 return Scheduler.newFixedDelaySchedule(0, duration.toMillis(), TimeUnit.MILLISECONDS);
 *             }
 *         };
 *     }
 * }).start();
 * </pre>
 * </p>
 */
public class LeaderService extends AbstractExecutionThreadService {
    private static final Logger LOG = LoggerFactory.getLogger(LeaderService.class);

    private final ConnectionStateListener _listener = new ConnectionStateListener() {
        @Override
        public void stateChanged(CuratorFramework curatorFramework, ConnectionState newState) {
            if (newState == ConnectionState.LOST || newState == ConnectionState.SUSPENDED) {
                LOG.debug("Lost leadership due to ZK state change to {}: {}", newState, getId());
                closeLeaderLatch();
            }
        }
    };
    private final CuratorFramework _curator;
    private final String _leaderPath;
    private final String _instanceId;
    private final String _serviceName;
    private final Supplier<Service> _serviceFactory;
    private final long _reacquireDelayNanos;
    private volatile LeaderLatch _latch;
    private volatile Service _delegate;

    /**
     * Creates an instance of the service that will create, start and stop a managed delegate service as this
     * instance acquires and loses leadership in a leadership election.
     * @param curator A started connection to ZooKeeper.
     * @param leaderPath The ZooKeeper path under which the leadership election algorithm will create ephemeral
     *                   ZooKeeper nodes.
     * @param instanceId An identifier for this instance, included in the information returned by the
     *                  {@link #getLeader()} and {@link #getParticipants()} instances.
     * @param serviceName The name of this service.  This will be used to name a Java thread dedicated to the
     *                    leadership election algorithm for this instance.
     * @param reacquireDelay The amount of time to wait before attempting to re-acquire leadership after losing
     *                       leadership due to ZooKeeper connection loss or after relinquishing leadership due to
     *                       another process manually stopping the managed delegate service.  It may be desirable
     *                       to set this to a relatively high value for services that are expensive to start to
     *                       avoid a rapid sequence of restarts in the presence of network issues that cause the
     *                       connection to ZooKeeper to flap back and forth.
     * @param reacquireDelayUnit The unit of the <code>reacquireDelay</code> argument.
     * @param serviceFactory A factory for delegate service instances.  This factory will be used to create a new
     *                       instance of the delegate service each time leadership is acquired.  Because Guava
     *                       services cannot be restarted, a new instance of the delegate service must be created
     *                       each time leadership is acquired.
     */
    public LeaderService(CuratorFramework curator, String leaderPath, String instanceId, String serviceName,
                         long reacquireDelay, TimeUnit reacquireDelayUnit, Supplier<Service> serviceFactory) {
        _curator = checkNotNull(curator, "curator");
        _leaderPath = checkNotNull(leaderPath, "leaderPath");
        _instanceId = checkNotNull(instanceId, "instanceId");
        _serviceName = checkNotNull(serviceName, "serviceName");
        _serviceFactory = checkNotNull(serviceFactory, "serviceFactory");
        _reacquireDelayNanos = checkNotNull(reacquireDelayUnit, "reacquireDelayUnit").toNanos(reacquireDelay);
        checkArgument(_reacquireDelayNanos >= 0, "reacquireDelay must be non-negative");
        initLeaderLatch();
    }

    @Override
    protected String serviceName() {
        return _serviceName;
    }

    /**
     * @return This instance's participant id provided at construction time.  This will be the value returned
     * when {@link #getParticipants()} is called.
     */
    public String getId() {
        return _instanceId;
    }

    /**
     * @return The set of current participants in the leader selection.
     * <p>
     * <B>NOTE</B> - this method polls the ZooKeeper server. Therefore it may return a value that does not match
     * {@link #hasLeadership()} as hasLeadership returns a cached value.
     */
    public Collection<Participant> getParticipants() throws Exception {
        return _latch.getParticipants();
    }

    /**
     * @return The id for the current leader. If for some reason there is no current leader, a dummy participant
     * is returned.
     * <p>
     * <B>NOTE</B> - this method polls the ZooKeeper server. Therefore it may return a value that does not match
     * {@link #hasLeadership()} as hasLeadership returns a cached value.
     */
    public Participant getLeader() throws Exception {
        return _latch.getLeader();
    }

    /** @return True if leadership is currently held by this instance. */
    public boolean hasLeadership() {
        return _latch.hasLeadership();
    }

    /**
     * @return The current wrapped service instance, if any.  Returns {@link Optional#absent()} when this instance
     * does not own the leadership lock.
     */
    public Optional<Service> getCurrentDelegateService() {
        return _latch.hasLeadership() ? Optional.fromNullable(_delegate) : Optional.<Service>absent();
    }

    @Override
    protected void startUp() throws Exception {
        _curator.getConnectionStateListenable().addListener(_listener);
    }

    @Override
    protected void shutDown() throws Exception {
        _curator.getConnectionStateListenable().removeListener(_listener);
    }

    @Override
    protected void triggerShutdown() {
        // Release leadership (if we have it) and wake up the main execution thread (if it's sleeping).
        closeLeaderLatch();
    }

    @Override
    protected void run() throws InterruptedException {
        // Beware race conditions: closeLeaderLatch() may be called by another thread at any time.
        while (isRunning()) {
            try {
                // Start attempting to acquire leadership via the Curator leadership latch.
                LOG.debug("Attempting to acquire leadership: {}", getId());
                LeaderLatch latch = startLeaderLatch();

                // Wait until (a) leadership is acquired or (b) the latch is closed by service shutdown or ZK cxn loss.
                if (isRunning()) {
                    try {
                        latch.await();
                    } catch (EOFException e) {
                        // Latch was closed while we were waiting.
                        checkState(!latch.hasLeadership());
                    }
                }

                // If we succeeded in acquiring leadership, start/run the leadership-managed delegate service.
                if (isRunning() && latch.hasLeadership()) {
                    LOG.debug("Leadership acquired: {}", getId());
                    runAsLeader();
                    LOG.debug("Leadership released: {}", getId());
                }
            } finally {
                closeLeaderLatch();
            }

            if (isRunning()) {
                // If we lost or relinquished leadership, wait a while for things to settle before trying to
                // re-acquire leadership (eg. wait for a network hiccup to the ZooKeeper server to resolve).
                sleep(_reacquireDelayNanos);
            }
        }
    }

    private void runAsLeader() throws InterruptedException {
        try {
            _delegate = listenTo(_serviceFactory.get());
            _delegate.startAndWait();
            try {
                awaitLeadershipLostOrServicesStopped();
            } finally {
                _delegate.stopAndWait();
            }
        } catch (InterruptedException ie) {
            throw ie;
        } catch (Throwable t) {
            // Start may have failed due to a network error, we'll sleep for reacquireDelay and try again.
            LOG.error("Exception starting or stopping leadership-managed service: {}", getId(), t);
        } finally {
            _delegate = null;
        }
    }

    private LeaderLatch newLeaderLatch() {
        return new LeaderLatch(_curator, _leaderPath, _instanceId);
    }

    // IMPORTANT: **************************************************************************************************
    // All updates to the '_latch' object are synchronized to avoid race conditions between the service execution
    // thread and the various listeners (ZK connection state listener, delegate service listener).

    private synchronized void initLeaderLatch() {
        // Create a non-started latch that we can use to implement getLeader(), getParticipants() & friends.
        _latch = newLeaderLatch();
    }

    private synchronized LeaderLatch startLeaderLatch() throws InterruptedException {
        LeaderLatch latch = _latch; // Read the volatile once
        // Assert not started already.  initLeaderLatch() and closeLeaderLatch() leave the latch in the latent state.
        checkState(latch.getState() == LeaderLatch.State.LATENT);
        try {
            latch.start();
        } catch (InterruptedException ie) {
            throw ie;
        } catch (Throwable t) {
            LOG.error("Exception attempting to acquire leadership: {}", getId(), t);
        }
        return latch;
    }

    private synchronized void closeLeaderLatch() {
        LeaderLatch latch = _latch; // Read the volatile once
        if (latch.getState() == LeaderLatch.State.STARTED) {
            try {
                latch.close();
            } catch (IOException e) {
                LOG.debug("Unexpected exception closing LeaderLatch.", e);
            }
        }
        // Return the latch to a latent state (newly created) for use by getLeader(), getParticipants() & friends.
        _latch = newLeaderLatch();
        // Wake up the main execution thread.
        notifyAll();
    }

    /** Wait until we lose leadership or this service is stopped or the delegate service is stopped. */
    private synchronized void awaitLeadershipLostOrServicesStopped() throws InterruptedException {
        while (_latch.hasLeadership() && isRunning() && _delegate.isRunning()) {
            wait();
        }
    }

    /** Wait for the specified amount of time or until this service is stopped, whichever comes first. */
    private synchronized void sleep(long waitNanos) throws InterruptedException {
        while (waitNanos > 0 && isRunning()) {
            long start = System.nanoTime();
            TimeUnit.NANOSECONDS.timedWait(this, waitNanos);
            waitNanos -= System.nanoTime() - start;
        }
    }

    /** Release leadership when the service terminates (normally or abnormally). */
    private Service listenTo(Service delegate) {
        delegate.addListener(new Listener() {
            @Override
            public void starting() {
                // Do nothing
            }

            @Override
            public void running() {
                // Do nothing
            }

            @Override
            public void stopping(State from) {
                // Do nothing
            }

            @Override
            public void terminated(State from) {
                closeLeaderLatch();
            }

            @Override
            public void failed(State from, Throwable failure) {
                closeLeaderLatch();
            }
        }, MoreExecutors.sameThreadExecutor());
        return delegate;
    }
}
