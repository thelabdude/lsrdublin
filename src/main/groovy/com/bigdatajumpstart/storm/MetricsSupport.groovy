package com.bigdatajumpstart.storm

import com.codahale.metrics.ConsoleReporter
import com.codahale.metrics.Meter
import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.Timer

import java.util.concurrent.TimeUnit

/**
 * Static helper for working with coda hale metrics
 */
class MetricsSupport {

    private static final MetricRegistry metricRegistry = new MetricRegistry()

    private static ConsoleReporter reporter = null

    static Timer timer(Class clazz, String timerName) {
        init()
        return metricRegistry.timer(MetricRegistry.name(clazz.simpleName, timerName))
    }

    static Meter meter(Class clazz, String timerName) {
        init()
        return metricRegistry.meter(MetricRegistry.name(clazz.simpleName, timerName))
    }


    static Object withTimer(Timer timer, Closure closure) {
        def context = timer.time()
        try {
            closure.call()
        } finally {
            context.stop()
        }
    }

    private static void init() {
        synchronized (MetricsSupport.class) {
            if (reporter == null) {
                reporter = ConsoleReporter.forRegistry(metricRegistry)
                        .convertRatesTo(TimeUnit.SECONDS)
                        .convertDurationsTo(TimeUnit.MILLISECONDS)
                        .build();
                reporter.start(1, TimeUnit.MINUTES);
            }
        }
    }
}
