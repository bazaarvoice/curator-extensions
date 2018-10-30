package com.bazaarvoice.curator.recipes.leader;

import com.bazaarvoice.curator.test.ZooKeeperTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.Participant;
import org.junit.Before;
import org.junit.Test;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LeaderServiceTest extends ZooKeeperTest {
    private static final String PATH = "/path/leader";

    private CuratorFramework _curator;

    @Before
    @Override
    public void setup() throws Exception {
        super.setup();
        _curator = newCurator();
    }

    private <T extends Service> T register(final T service) {
        closer().register(new Closeable() {
            @Override
            public void close() {
                service.stopAsync();
            }
        });
        return service;
    }

    private LeaderService newLeaderService(int reacquireDelay, TimeUnit reacquireDelayUnit, Supplier<Service> services) {
        return register(new LeaderService(
                _curator, PATH, "test-id", "LeaderService", reacquireDelay, reacquireDelayUnit, services));
    }


    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Tests
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    /** Test starting and stopping a single instance of LeaderService. */
    @Test
    public void testLifeCycle() throws Exception {
        ServiceTriggers triggers = new ServiceTriggers();
        LeaderService leader = newLeaderService(1, TimeUnit.HOURS, supply(triggers.listenTo(new NopService())));
        assertEquals("test-id", leader.getId());
        assertFalse(leader.hasLeadership());

        // Start trying to obtain leadership
        leader.startAsync();
        assertTrue(triggers.getRunning().firedWithin(1, TimeUnit.MINUTES));
        assertTrue(leader.isRunning());
        assertTrue(leader.hasLeadership());
        assertEquals(new Participant("test-id", true), leader.getLeader());
        assertEquals(Collections.singletonList(new Participant("test-id", true)), leader.getParticipants());
        assertFalse(triggers.getTerminated().hasFired());
        assertTrue(leader.getCurrentDelegateService().get().isRunning());

        // Start watching ZooKeeper directly for changes
        WatchTrigger childrenTrigger = WatchTrigger.childrenTrigger();
        _curator.getChildren().usingWatcher(childrenTrigger).forPath(PATH);

        // Stop trying to obtain leadership
        leader.stopAsync();
        assertTrue(triggers.getTerminated().firedWithin(1, TimeUnit.SECONDS));
        assertFalse(leader.isRunning());
        assertFalse(leader.getCurrentDelegateService().isPresent());

        // Wait for stopped state to reflect in ZooKeeper then poll ZooKeeper for leadership participants state
        assertTrue(childrenTrigger.firedWithin(1, TimeUnit.SECONDS));
        assertFalse(leader.hasLeadership());
        assertTrue(_curator.getChildren().forPath(PATH).isEmpty());
        assertEquals(new Participant("", false), leader.getLeader());
        assertEquals(Collections.<Participant>emptyList(), leader.getParticipants());
    }

    /** Test starting multiple instances that compete for leadership. */
    @Test
    public void testMultipleLeaders() throws Exception {
        final Trigger started = new Trigger();
        final AtomicInteger startCount = new AtomicInteger();
        for (int i = 0; i < 5; i++) {
            newLeaderService(1, TimeUnit.HOURS, new Supplier<Service>() {
                @Override
                public Service get() {
                    return new AbstractIdleService() {
                        @Override
                        protected void startUp() throws Exception {
                            started.fire();
                            startCount.incrementAndGet();
                        }

                        @Override
                        protected void shutDown() throws Exception {
                            // Do nothing
                        }
                    };
                }
            }).startAsync();
        }
        assertTrue(started.firedWithin(1, TimeUnit.MINUTES));
        // We know one service has started.  Wait a little while and verify no more services are started.
        Thread.sleep(250);
        assertTrue(startCount.get() == 1);
    }

    @Test
    public void testSelfStoppingService() {
        // If the delegate service stops itself then it gets restarted after reacquireDelay.
        int reacquireDelayMillis = 1500;
        ServiceTriggers triggers1 = new ServiceTriggers();
        ServiceTriggers triggers2 = new ServiceTriggers();
        LeaderService leader = newLeaderService(reacquireDelayMillis, TimeUnit.MILLISECONDS, supply(
                triggers1.listenTo(new AbstractScheduledService() {
                    @Override
                    protected void runOneIteration() throws Exception {
                        stopAsync();
                    }

                    @Override
                    protected Scheduler scheduler() {
                        return Scheduler.newFixedDelaySchedule(10, 10, TimeUnit.MILLISECONDS);
                    }
                }),
                triggers2.listenTo(new NopService())));
        leader.startAsync();

        assertTrue(triggers1.getRunning().firedWithin(1, TimeUnit.MINUTES));
        assertTrue(triggers1.getTerminated().firedWithin(1, TimeUnit.MINUTES));
        assertTrue(triggers2.getRunning().firedWithin(1, TimeUnit.MINUTES));
    }

    /** Verify that leadership is re-acquired after the connection to ZooKeeper is lost. */
    @Test
    public void testLostZooKeeperConnection() throws Exception {
        int reacquireDelayMillis = 1500;
        ServiceTriggers triggers1 = new ServiceTriggers();
        ServiceTriggers triggers2 = new ServiceTriggers();
        ServiceTimer timer1 = new ServiceTimer();
        ServiceTimer timer2 = new ServiceTimer();
        List<Event> events = Collections.synchronizedList(new ArrayList<>());
        Service leader = newLeaderService(reacquireDelayMillis, TimeUnit.MILLISECONDS, supply(
                triggers1.listenTo(timer1.listenTo(trackEvents("1", events, new NopService()))),
                triggers2.listenTo(timer2.listenTo(trackEvents("2", events, new NopService())))));

        leader.startAsync();
        assertTrue(triggers1.getRunning().firedWithin(1, TimeUnit.MINUTES));

        killSession(_curator);
        assertTrue(triggers1.getTerminated().firedWithin(1, TimeUnit.MINUTES));
        assertTrue(triggers2.getRunning().firedWithin(1, TimeUnit.MINUTES));

        leader.stopAsync();
        assertTrue(triggers2.getTerminated().firedWithin(1, TimeUnit.MINUTES));

        // Verify sequence of events, no overlap between service instances.
        assertEquals(ImmutableList.of(
                new Event("1", Service.State.STARTING),
                new Event("1", Service.State.RUNNING),
                new Event("1", Service.State.STOPPING),
                new Event("1", Service.State.TERMINATED),
                new Event("2", Service.State.STARTING),
                new Event("2", Service.State.RUNNING),
                new Event("2", Service.State.STOPPING),
                new Event("2", Service.State.TERMINATED)
        ), events);

        // Verify that the re-acquire delay was observed
        long actualDelayMillis = timer2.getStartedAt() - timer1.getStoppedAt();
        assertTrue("Re-acquire delay was not observed: " + actualDelayMillis, actualDelayMillis >= reacquireDelayMillis);
    }

    /** Verify that leadership is re-acquired after the the delegate service throws an exception at startup. */
    @Test
    public void testStartupFailed() throws Exception {
        int reacquireDelayMillis = 1500;
        ServiceTriggers triggers1 = new ServiceTriggers();
        ServiceTriggers triggers2 = new ServiceTriggers();
        ServiceTimer timer1 = new ServiceTimer();
        ServiceTimer timer2 = new ServiceTimer();
        List<Event> events = Collections.synchronizedList(new ArrayList<>());
        Service leader = newLeaderService(reacquireDelayMillis, TimeUnit.MILLISECONDS, supply(
                triggers1.listenTo(timer1.listenTo(trackEvents("1", events, new NopService() {
                    @Override
                    protected void startUp() throws Exception {
                        throw new Exception("Startup failed");
                    }
                }))),
                triggers2.listenTo(timer2.listenTo(trackEvents("2", events, new NopService())))));

        leader.startAsync();
        assertTrue(triggers1.getFailed().firedWithin(1, TimeUnit.MINUTES));
        assertTrue(triggers2.getRunning().firedWithin(1, TimeUnit.MINUTES));

        // Verify sequence of events, no overlap between service instances.
        assertEquals(ImmutableList.of(
                new Event("1", Service.State.STARTING),
                new Event("1", Service.State.FAILED),
                new Event("2", Service.State.STARTING),
                new Event("2", Service.State.RUNNING)
        ), events);

        // Verify that the re-acquire delay was observed
        long actualDelayMillis = timer2.getStartedAt() - timer1.getStoppedAt();
        assertTrue("Re-acquire delay was not observed: " + actualDelayMillis, actualDelayMillis >= reacquireDelayMillis);
    }

    /** Verify that leadership is re-acquired after the the delegate service throws an exception at shutdown. */
    @Test
    public void testShutdownFailed() throws Exception {
        int reacquireDelayMillis = 1500;
        ServiceTriggers triggers1 = new ServiceTriggers();
        ServiceTriggers triggers2 = new ServiceTriggers();
        ServiceTimer timer1 = new ServiceTimer();
        ServiceTimer timer2 = new ServiceTimer();
        List<Event> events = Collections.synchronizedList(new ArrayList<>());
        Service leader = newLeaderService(reacquireDelayMillis, TimeUnit.MILLISECONDS, supply(
                triggers1.listenTo(timer1.listenTo(trackEvents("1", events, new NopService() {
                    @Override
                    protected void shutDown() throws Exception {
                        throw new Exception("ShutDown failed");
                    }
                }))),
                triggers2.listenTo(timer2.listenTo(trackEvents("2", events, new NopService())))));

        leader.startAsync();
        assertTrue(triggers1.getRunning().firedWithin(1, TimeUnit.MINUTES));

        killSession(_curator);
        assertTrue(triggers1.getFailed().firedWithin(1, TimeUnit.MINUTES));
        assertTrue(triggers2.getRunning().firedWithin(1, TimeUnit.MINUTES));

        // Verify sequence of events, no overlap between service instances.
        assertEquals(ImmutableList.of(
                new Event("1", Service.State.STARTING),
                new Event("1", Service.State.RUNNING),
                new Event("1", Service.State.STOPPING),
                new Event("1", Service.State.FAILED),
                new Event("2", Service.State.STARTING),
                new Event("2", Service.State.RUNNING)
        ), events);

        // Verify that the re-acquire delay was observed
        long actualDelayMillis = timer2.getStartedAt() - timer1.getStoppedAt();
        assertTrue("Re-acquire delay was not observed: " + actualDelayMillis, actualDelayMillis >= reacquireDelayMillis);
    }

    /** Verify that leadership is re-acquired after the the delegate service throws an exception at shutdown. */
    @Test
    public void testShutdownDuringReacquireDelay() throws Exception {
        ServiceTriggers leaderTriggers = new ServiceTriggers();
        ServiceTriggers triggers1 = new ServiceTriggers();
        ServiceTriggers triggers2 = new ServiceTriggers();
        LeaderService leader = leaderTriggers.listenTo(newLeaderService(15, TimeUnit.MINUTES, supply(
                triggers1.listenTo(new NopService()),
                triggers2.listenTo(new NopService()))));

        leader.startAsync();
        assertTrue(triggers1.getRunning().firedWithin(1, TimeUnit.MINUTES));
        assertTrue(leader.hasLeadership());

        leader.getCurrentDelegateService().get().stopAsync();
        assertTrue(triggers1.getTerminated().firedWithin(1, TimeUnit.SECONDS));

        // Should be waiting for the reacquire delay now.  Make sure we don't immediately start the 2nd service.
        Thread.sleep(50);
        assertFalse(leader.hasLeadership());
        assertFalse(triggers2.getStarting().hasFired());

        // Stop the leader service and verify that we interrupt the reacquire delay sleep.
        assertFalse(leaderTriggers.getTerminated().hasFired());
        leader.stopAsync();
        assertTrue(leaderTriggers.getTerminated().firedWithin(1, TimeUnit.SECONDS));

        // One last check that the 2nd service was never started...
        assertFalse(triggers2.getStarting().hasFired());
    }

    /** Verify that the name of the thread created by LeaderSelector is set correctly. */
    @Test
    public void testThreadName() throws Exception {
        final String expectedThreadName = "TestLeaderService";
        final SettableFuture<String> actualThreadName = SettableFuture.create();
        register(new LeaderService(_curator, PATH, "id", expectedThreadName, 1, TimeUnit.HOURS, () -> new AbstractService() {
            @Override
            protected void doStart() {
                actualThreadName.set(Thread.currentThread().getName());
                notifyStarted();
            }

            @Override
            protected void doStop() {
                notifyStopped();
            }
        })).startAsync();
        assertEquals(expectedThreadName, actualThreadName.get(1, TimeUnit.MINUTES));
    }

    private static Service trackEvents(String id, List<Event> events, Service service) {
        service.addListener(new EventListener(id, events), MoreExecutors.sameThreadExecutor());
        return service;
    }

    private static Supplier<Service> supply(Service... services) {
        final Iterator<Service> iter = Iterators.forArray(services);
        return new Supplier<Service>() {
            @Override
            public Service get() {
                return iter.next();
            }
        };
    }

    private static class NopService extends AbstractIdleService {
        @Override
        protected void startUp() throws Exception {}

        @Override
        protected void shutDown() throws Exception {}
    }

    private static class ServiceTriggers extends Service.Listener {
        private final Trigger _starting = new Trigger();
        private final Trigger _running = new Trigger();
        private final Trigger _stopping = new Trigger();
        private final Trigger _terminated = new Trigger();
        private final Trigger _failed = new Trigger();

        public <T extends Service> T listenTo(T service) {
            service.addListener(this, MoreExecutors.sameThreadExecutor());
            return service;
        }

        @Override
        public void starting() {
            _starting.fire();
        }

        @Override
        public void running() {
            _running.fire();
        }

        @Override
        public void stopping(Service.State from) {
            _stopping.fire();
        }

        @Override
        public void terminated(Service.State from) {
            _terminated.fire();
        }

        @Override
        public void failed(Service.State from, Throwable failure) {
            _failed.fire();
        }

        public Trigger getStarting() {
            return _starting;
        }

        public Trigger getRunning() {
            return _running;
        }

        public Trigger getTerminated() {
            return _terminated;
        }

        public Trigger getFailed() {
            return _failed;
        }
    }

    private static class ServiceTimer extends Service.Listener {
        private Long _startedAt;
        private Long _stoppedAt;

        public <T extends Service> T listenTo(T service) {
            service.addListener(this, MoreExecutors.sameThreadExecutor());
            return service;
        }

        @Override
        public void running() {
            _startedAt = System.currentTimeMillis();
        }

        @Override
        public void terminated(Service.State from) {
            _stoppedAt = System.currentTimeMillis();
        }

        @Override
        public void failed(Service.State from, Throwable failure) {
            _stoppedAt = System.currentTimeMillis();
        }

        public Long getStartedAt() {
            return _startedAt;
        }

        public Long getStoppedAt() {
            return _stoppedAt;
        }
    }

    private static class EventListener extends Service.Listener {
        private final String _id;
        private final List<Event> _events;

        private EventListener(String id, List<Event> events) {
            _events = events;
            _id = id;
        }

        private boolean addEvent(Service.State state) {
            return _events.add(new Event(_id, state));
        }

        @Override
        public void starting() {
            addEvent(Service.State.STARTING);
        }

        @Override
        public void running() {
            addEvent(Service.State.RUNNING);
        }

        @Override
        public void stopping(Service.State from) {
            addEvent(Service.State.STOPPING);
        }

        @Override
        public void terminated(Service.State from) {
            addEvent(Service.State.TERMINATED);
        }

        @Override
        public void failed(Service.State from, Throwable failure) {
            addEvent(Service.State.FAILED);
        }
    }

    private static class Event {
        private final String _id;
        private final Service.State _state;

        private Event(String id, Service.State state) {
            _id = Objects.requireNonNull(id, "id");
            _state = Objects.requireNonNull(state, "state");
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Event)) {
                return false;
            }
            Event that = (Event) o;
            return _id.equals(that._id) && _state == that._state;

        }

        @Override
        public int hashCode() {
            return Objects.hash(_id, _state);
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", Event.class.getSimpleName() + "[", "]")
                    .add("_id='" + _id + "'")
                    .add("_state=" + _state)
                    .toString();
        }
    }
}
