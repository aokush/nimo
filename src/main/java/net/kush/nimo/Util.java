package net.kush.nimo;

import it.sauronsoftware.cron4j.Scheduler;

public class Util {

    private static Scheduler scheduler;

    private Util(){}

    public static synchronized Scheduler getOrCreateScheduler() {
        if (scheduler == null) {
            scheduler = new Scheduler();
            scheduler.start();
            Runtime.getRuntime().addShutdownHook(new Thread(scheduler::stop));
        }

        return scheduler;
    }

    public static void deschedule(String scheduleId) {
        if (scheduler != null && scheduler.isStarted()) {
            scheduler.deschedule(scheduleId);
        }        
    }

}
