package net.kush.nimo;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 *
 * @author Home
 */
public class JNDILoaderTest {

    private static Context ctx;

    private static final String TABLE_NAME = "KEYVALUE";

    private static final String KEY_1 = "prop1";
    private static final String KEY_2 = "prop2";
    private static final String KEY_3 = "prop3";
    private static final String VALUE_1 = "Line1";
    private static final String VALUE_2 = "Line2";
    private static final String VALUE_3 = "Line3";
    private static final String VALUE_1_CHANGED = "Line1-Changed";
    private static final String VALUE_2_CHANGED = "Line2-Changed";

    public JNDILoaderTest() {
    }

    @BeforeAll
    public static void setUpClass() throws Exception {

    }

    @AfterAll
    public static void tearDownClass() throws Exception {

    }

    @BeforeEach
    public void setUp() throws NamingException, RemoteException {

        Map<String, String> props = new HashMap<String, String>();
        props.put(KEY_1, VALUE_1);
        props.put(KEY_2, VALUE_2);

        // ctx.rebind(TABLE_NAME, props);

        ctx = mock(InitialContext.class);

        when(ctx.lookup(TABLE_NAME)).thenReturn(props);
    }

    @AfterEach
    public void tearDown() throws NamingException {
        ctx.unbind(TABLE_NAME);
    }

    @Test
    public void testSelectAllQuery() throws Exception {
        JNDILoader instance = new JNDILoader(ctx, TABLE_NAME);
        String result = instance.getProperty(KEY_2);
        assertEquals(VALUE_2, result);
    }

    /**
     * Test of getProperty method, of class JNDILoader.
     */
    @Test
    public void testGetProperty() throws Exception {
        JNDILoader instance = new JNDILoader(ctx, TABLE_NAME);
        String result = instance.getProperty(KEY_2);
        assertEquals(VALUE_2, result);
    }

    /**
     * Test of getProperties method, of class JNDILoader.
     */
    @Test
    public void testGetProperties() throws Exception {
        JNDILoader instance = new JNDILoader(ctx, TABLE_NAME);

        Map<String, String> expResult = new HashMap<String, String>();
        expResult.put(KEY_1, VALUE_1);
        expResult.put(KEY_2, VALUE_2);

        Map<String, String> result = instance.getProperties();
        assertEquals(expResult, result);
    }

    @Test
    public void testGetPropertyForDefaultReload() throws Exception {
        JNDILoader instance = new JNDILoader(ctx, TABLE_NAME);

        Map<String, String> props = new HashMap<String, String>();
        props.put(KEY_1, VALUE_1);
        props.put(KEY_2, VALUE_2_CHANGED);

        when(ctx.lookup(TABLE_NAME)).thenReturn(props);
        TestUtils.maxWaitFor(70).untilAsserted(() -> assertEquals(VALUE_2_CHANGED, instance.getProperty(KEY_2)));
    }

    @Test
    public void testGetPropertyForSpecificReload() throws Exception {
        JNDILoader instance = new JNDILoader(ctx, TABLE_NAME, 2);

        Map<String, String> props = new HashMap<String, String>();
        props.put(KEY_1, VALUE_1);
        props.put(KEY_2, VALUE_2_CHANGED);

        when(ctx.lookup(TABLE_NAME)).thenReturn(props);
        TestUtils.maxWaitFor(130).untilAsserted(() -> assertEquals(VALUE_2_CHANGED, instance.getProperty(KEY_2)));
    }

    @Test
    public void testGetPropertiesForDefaultReload() throws Exception {
        JNDILoader instance = new JNDILoader(ctx, TABLE_NAME);

        Map<String, String> expResult = new HashMap<String, String>();
        expResult.put(KEY_1, VALUE_1);
        expResult.put(KEY_2, VALUE_2);

        Map<String, String> result = instance.getProperties();
        assertEquals(expResult, result);

        Map<String, String> props = new HashMap<String, String>();
        props.put(KEY_1, VALUE_1_CHANGED);
        props.put(KEY_2, VALUE_2_CHANGED);
        props.put(KEY_3, VALUE_3);

        when(ctx.lookup(TABLE_NAME)).thenReturn(props);

        expResult.clear();
        expResult.put(KEY_1, VALUE_1_CHANGED);
        expResult.put(KEY_2, VALUE_2_CHANGED);
        expResult.put(KEY_3, VALUE_3);

        TestUtils.maxWaitFor(70).untilAsserted(() -> assertEquals(expResult, instance.getProperties()));
    }

    @Test
    public void testGetPropertiesorSpecificReload() throws Exception {
        JNDILoader instance = new JNDILoader(ctx, TABLE_NAME, 2);

        Map<String, String> expResult = new HashMap<String, String>();
        expResult.put(KEY_1, VALUE_1);
        expResult.put(KEY_2, VALUE_2);

        Map<String, String> result = instance.getProperties();
        assertEquals(expResult, result);

        Map<String, String> props = new HashMap<String, String>();
        props.put(KEY_1, VALUE_1_CHANGED);
        props.put(KEY_2, VALUE_2_CHANGED);
        props.put(KEY_3, VALUE_3);

        when(ctx.lookup(TABLE_NAME)).thenReturn(props);

        expResult.clear();
        expResult.put(KEY_1, VALUE_1_CHANGED);
        expResult.put(KEY_2, VALUE_2_CHANGED);
        expResult.put(KEY_3, VALUE_3);

        TestUtils.maxWaitFor(130).untilAsserted(() -> assertEquals(expResult, instance.getProperties()));
    }

    @Test
    public void testNullNameload() throws Exception {
        assertThrows(PropertyException.class, () -> new JNDILoader(ctx, null));
    }

    @Test
    public void testNullJNDIContext() throws Exception {
        assertThrows(PropertyException.class, () -> new JNDILoader(null, TABLE_NAME));
    }

    @Test
    public void testInvalidReloadInterval() throws Exception {
        assertThrows(PropertyException.class, () -> new JNDILoader(ctx, TABLE_NAME, 0));
    }
}