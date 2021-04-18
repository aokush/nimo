package net.kush.nimo;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;

/**
 *
 * @author Home
 */
public class UtilTest {


    @Test
    public void testGetOrCreateScheduler() throws Exception {
        assertNotNull(Util.getOrCreateScheduler(), "Scheduler should not be null");
    }
   
}