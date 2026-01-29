package com.compare.common;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class LatencyRecorder {
    private final List<Long> latencies = Collections.synchronizedList(new ArrayList<>());
    private final AtomicLong totalLatency = new AtomicLong(0);
    private final AtomicLong minLatency = new AtomicLong(Long.MAX_VALUE);
    private final AtomicLong maxLatency = new AtomicLong(0);
    private final AtomicLong count = new AtomicLong(0);

    public void record(long latency) {
        if (latency < 0) return;
        
        latencies.add(latency);
        totalLatency.addAndGet(latency);
        
        long currentMin = minLatency.get();
        while (latency < currentMin && !minLatency.compareAndSet(currentMin, latency)) {
            currentMin = minLatency.get();
        }

        long currentMax = maxLatency.get();
        while (latency > currentMax && !maxLatency.compareAndSet(currentMax, latency)) {
            currentMax = maxLatency.get();
        }
        
        count.incrementAndGet();
    }

    public double getAverage() {
        long c = count.get();
        if (c == 0) return 0;
        return (double) totalLatency.get() / c;
    }

    public long getMin() {
        long min = minLatency.get();
        return min == Long.MAX_VALUE ? 0 : min;
    }

    public long getMax() {
        return maxLatency.get();
    }

    public long getCount() {
        return count.get();
    }

    public void reset() {
        latencies.clear();
        totalLatency.set(0);
        minLatency.set(Long.MAX_VALUE);
        maxLatency.set(0);
        count.set(0);
    }

    public List<Long> getLatencies() {
        return new ArrayList<>(latencies);
    }
}
