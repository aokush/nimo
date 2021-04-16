package net.kush.nimo;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;

import it.sauronsoftware.cron4j.Scheduler;

/**
 *
 * @author Home
 */
public class UtilTest {


    @Test
    public void testGetOrCreateScheduler() throws Exception {
        assertNotNull(Util.getOrCreateScheduler(), "Scheduler should not be null");
    }

    /**
     * Test of getProperty method, of class JNDILoader.
     */
    @Test
    public void testDeschedule() throws Exception {
        Scheduler scheduler = Util.getOrCreateScheduler();
        Runnable task = () -> System.out.print("");
        final String pattern = "*/1 * * * *";

        String firstScheduleId = scheduler.schedule(pattern, task);
        Util.deschedule(firstScheduleId);
        String secondScheduleId = scheduler.schedule(pattern, task);
        Util.deschedule(secondScheduleId);

        assertNotEquals(firstScheduleId, secondScheduleId);

    }

   
}