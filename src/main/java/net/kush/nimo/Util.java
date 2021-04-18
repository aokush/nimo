package net.kush.nimo;

import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A utility for common operations
 *
 * @author Adebiyi Kuseju (Kush)
 */
public class Util {

    private static final AtomicInteger THREAD_COUNTER = new AtomicInteger();
    private static ScheduledExecutorService scheduler;
    private static final int THREAD_POOL_SIZE = Integer.parseInt(System.getProperty("nimo.workers.size", "2"));

    private Util() {
    }

    public static synchronized ScheduledExecutorService getOrCreateScheduler() {
        if (scheduler == null) {
            scheduler = Executors.newScheduledThreadPool(THREAD_POOL_SIZE, r -> {

                Thread t = new Thread(r);
                t.setName("NIMO-THREAD-POOL-" + THREAD_COUNTER.get());
                return t;

            });

            Runtime.getRuntime().addShutdownHook(new Thread(() -> scheduler.shutdownNow()));
        }

        return scheduler;
    }

    public static void deschedule(Future<?> scheduledFuture) {
        if (scheduledFuture != null) {
            scheduledFuture.cancel(true);
        }
    }

    public static void validateArgs(Reloadable.RELOAD_STRATEGY reloadStrategy,
            Reloadable.UPDATE_STRATEGY updateStrategy, long interval) throws PropertyException {

        if (reloadStrategy == null) {
            throw new PropertyException("Inavlid 'reloadStrategy'");
        }

        if (updateStrategy == null) {
            throw new PropertyException("Inavlid 'updateStrategy'");
        }

        Util.validateArgs(interval);
    }

    public static void validateArgs(long interval) throws PropertyException {

        if (interval <= 0) {
            throw new PropertyException("Interval must be greater than zero");
        }
    }

}
