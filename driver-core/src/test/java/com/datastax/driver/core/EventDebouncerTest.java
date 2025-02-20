/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core;

import com.datastax.driver.core.EventDebouncer.DeliveryCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;

public class EventDebouncerTest {

    private ScheduledExecutorService executor;

    private MockDeliveryCallback callback;

    @BeforeMethod(groups = "unit")
    public void setup() {
        executor = Executors.newScheduledThreadPool(1);
        callback = new MockDeliveryCallback();
    }

    @AfterMethod(groups = "unit")
    public void tearDown() {
        executor.shutdownNow();
    }

    @Test(groups = "unit")
    public void should_deliver_single_event() throws InterruptedException {
        EventDebouncer<MockEvent> debouncer = new EventDebouncer<MockEvent>("test", executor, callback, 10, 50);
        debouncer.start();
        MockEvent event = new MockEvent(0);
        debouncer.eventReceived(event);
        callback.awaitEvents(1);
        assertThat(callback.getEvents()).containsOnly(event);
    }

    @Test(groups = "unit")
    public void should_deliver_n_events_in_order() throws InterruptedException {
        EventDebouncer<MockEvent> debouncer = new EventDebouncer<MockEvent>("test", executor, callback, 10, 50);
        debouncer.start();
        List<MockEvent> events = new ArrayList<MockEvent>();
        for (int i = 0; i < 50; i++) {
            MockEvent event = new MockEvent(i);
            events.add(event);
            debouncer.eventReceived(event);
        }
        callback.awaitEvents(50);
        assertThat(callback.getEvents()).isEqualTo(events);
        assertThat(callback.getInvocations()).isEqualTo(1);
    }

    @Test(groups = "unit")
    public void should_deliver_n_events_in_order_even_if_queue_full() throws InterruptedException {
        EventDebouncer<MockEvent> debouncer = new EventDebouncer<MockEvent>("test", executor, callback, 10, 1);
        debouncer.start();
        List<MockEvent> events = new ArrayList<MockEvent>();
        for (int i = 0; i < 50; i++) {
            MockEvent event = new MockEvent(i);
            events.add(event);
            debouncer.eventReceived(event);
        }
        callback.awaitEvents(50);
        assertThat(callback.getEvents()).isEqualTo(events);
    }

    @Test(groups = "unit")
    public void should_accumulate_events_if_not_ready() throws InterruptedException {
        EventDebouncer<MockEvent> debouncer = new EventDebouncer<MockEvent>("test", executor, callback, 10, 50);
        List<MockEvent> events = new ArrayList<MockEvent>();
        for (int i = 0; i < 50; i++) {
            MockEvent event = new MockEvent(i);
            events.add(event);
            debouncer.eventReceived(event);
        }
        // simulate late start
        debouncer.start();
        callback.awaitEvents(50);
        assertThat(callback.getEvents()).hasSize(50);
        assertThat(callback.getEvents()).isEqualTo(events);
    }

    @Test(groups = "unit")
    public void should_accumulate_all_events_until_start() throws InterruptedException {
        final EventDebouncer<MockEvent> debouncer = new EventDebouncer<MockEvent>("test", executor, callback, 10, 25);
        final List<MockEvent> events = new ArrayList<MockEvent>();

        for (int i = 0; i < 50; i++) {
            MockEvent event = new MockEvent(i);
            events.add(event);
            debouncer.eventReceived(event);
        }

        debouncer.start();

        callback.awaitEvents(50);
        assertThat(callback.getEvents()).isEqualTo(events);
        assertThat(callback.getInvocations()).isEqualTo(1);
    }

    @Test(groups = "unit")
    public void should_reset_timer_if_n_events_received_within_same_window() throws InterruptedException {
        final EventDebouncer<MockEvent> debouncer = new EventDebouncer<MockEvent>("test", executor, callback, 50, 50);
        debouncer.start();
        final CountDownLatch latch = new CountDownLatch(50);
        ScheduledExecutorService pool = Executors.newScheduledThreadPool(1);
        pool.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                if (latch.getCount() > 0) {
                    MockEvent event = new MockEvent(0);
                    debouncer.eventReceived(event);
                    latch.countDown();
                }
            }
        }, 0, 5, MILLISECONDS);
        latch.await();
        pool.shutdownNow();
        callback.awaitEvents(50);
        assertThat(callback.getEvents()).hasSize(50);
        assertThat(callback.getInvocations()).isEqualTo(1);
    }

    @Test(groups = "unit")
    public void should_stop_receiving_events() throws InterruptedException {
        final EventDebouncer<MockEvent> debouncer = new EventDebouncer<MockEvent>("test", executor, callback, 10, 50);
        debouncer.start();
        for (int i = 0; i < 50; i++) {
            MockEvent event = new MockEvent(i);
            debouncer.eventReceived(event);
        }
        callback.awaitEvents(50);
        debouncer.stop();
        MockEvent event = new MockEvent(0);
        debouncer.eventReceived(event);
        assertThat(callback.getEvents()).hasSize(50);
        assertThat(callback.getInvocations()).isEqualTo(1);
    }

    private static class MockDeliveryCallback implements DeliveryCallback<MockEvent> {

        final List<MockEvent> events = new CopyOnWriteArrayList<MockEvent>();

        final Lock lock = new ReentrantLock();

        final Condition cond = lock.newCondition();

        final AtomicInteger invocations = new AtomicInteger(0);

        @Override
        public ListenableFuture<?> deliver(List<MockEvent> events) {
            lock.lock();
            try {
                this.events.addAll(events);
                invocations.incrementAndGet();
                cond.signal();
            } finally {
                lock.unlock();
            }
            return Futures.immediateFuture(null);
        }

        void awaitEvents(int expected) throws InterruptedException {
            long nanos = MINUTES.toNanos(5);
            lock.lock();
            try {
                while (events.size() < expected) {
                    if (nanos <= 0L)
                        break; // timeout
                    nanos = cond.awaitNanos(nanos);
                }
            } finally {
                lock.unlock();
            }
        }

        public List<MockEvent> getEvents() {
            return events;
        }

        public int getInvocations() {
            return invocations.get();
        }
    }

    private class MockEvent {

        private final int i;

        private MockEvent(int i) {
            this.i = i;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            MockEvent mockEvent = (MockEvent) o;

            return i == mockEvent.i;

        }

        @Override
        public int hashCode() {
            return i;
        }

        @Override
        public String toString() {
            return "MockEvent" + i;
        }
    }
}
