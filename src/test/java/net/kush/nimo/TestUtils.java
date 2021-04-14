package net.kush.nimo;

import java.util.concurrent.TimeUnit;
import static org.awaitility.Awaitility.await;
import org.awaitility.core.ConditionFactory;

public class TestUtils {

    public static ConditionFactory maxWaitFor(int seconds) {
        return await().atMost(seconds, TimeUnit.SECONDS);
    }
    
}
