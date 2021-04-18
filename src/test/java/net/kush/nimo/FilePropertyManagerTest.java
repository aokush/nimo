package net.kush.nimo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 *
 * @author Home
 */
public class FilePropertyManagerTest {

    private File file;

    private static final String KEY_1 = "prop1";
    private static final String KEY_2 = "prop2";
    private static final String VALUE_1 = "Line1";
    private static final String VALUE_2 = "Line2";

    @BeforeEach
    public void setUp() throws Exception {
        file = File.createTempFile("FileLoader", ".txt");
        BufferedWriter writer = getWriterForFile();

        Properties prop = new Properties();
        prop.put(KEY_1, VALUE_1);
        prop.put(KEY_2, VALUE_2);
        prop.store(writer, null);

        deallocate(writer);

    }

    @AfterEach
    public void tearDown() {
        if (file != null) {
            file.delete();
        }
    }

    @Test
    public void testGetProperty_No_reload() throws Exception {

        Reloadable instance = new FilePropertyManager(file.getAbsolutePath());

        Map<String, String> expResult = new HashMap<String, String>();
        expResult.put(KEY_1, VALUE_1);
        expResult.put(KEY_2, VALUE_2);

        Map<String, String> result = instance.getProperties();
        assertEquals(expResult, result);

        Map<String, String> newProps = new HashMap<String, String>();
        newProps.put("newProps1", "Line1");
        newProps.put("newProps2", "Line2");
        newProps.put("prop3", "Line3");
        newProps.put("prop4", "Line4");

        instance.setProperties(newProps, true);

        assertNull(instance.getProperty(KEY_1));
        assertNull(instance.getProperty(KEY_2));
        assertEquals("Line1", instance.getProperty("newProps1"));
        assertEquals("Line2", instance.getProperty("newProps2"));
        assertEquals("Line3", instance.getProperty("prop3"));
        assertEquals("Line4", instance.getProperty("prop4"));
        instance.close();
    }

    @Test
    public void testGetProperty_UpdateAndPersistStrategy_Refresh_true() throws Exception {

        Reloadable instance = new FilePropertyManager(file.getAbsolutePath(), Reloadable.RELOAD_STRATEGY.NONE,
                Reloadable.UPDATE_STRATEGY.INTERNAL);

        Map<String, String> expResult = new HashMap<String, String>();
        expResult.put(KEY_1, VALUE_1);
        expResult.put(KEY_2, VALUE_2);

        Map<String, String> result = instance.getProperties();
        assertEquals(expResult, result);

        Map<String, String> newProps = new HashMap<String, String>();
        newProps.put("newProps1", "Line1");
        newProps.put("newProps2", "Line2");
        newProps.put("prop3", "Line3");
        newProps.put("prop4", "Line4");

        instance.setProperties(newProps, true);

        assertNull(instance.getProperty(KEY_1));
        assertNull(instance.getProperty(KEY_2));
        assertEquals("Line1", instance.getProperty("newProps1"));
        assertEquals("Line2", instance.getProperty("newProps2"));
        assertEquals("Line3", instance.getProperty("prop3"));
        assertEquals("Line4", instance.getProperty("prop4"));
        instance.close();
    }

    @Test
    public void testGetProperties_UpdateAndPersistStrategy_Refresh_true() throws Exception {

        Reloadable instance = new FilePropertyManager(file.getAbsolutePath(), Reloadable.RELOAD_STRATEGY.NONE,
                Reloadable.UPDATE_STRATEGY.INTERNAL);

        Map<String, String> expResult = new HashMap<String, String>();
        expResult.put(KEY_1, VALUE_1);
        expResult.put(KEY_2, VALUE_2);

        Map<String, String> result = instance.getProperties();
        assertEquals(expResult, result);

        Map<String, String> newProps = new HashMap<String, String>();
        newProps.put("newProps1", "Line1");
        newProps.put("newProps2", "Line2");
        newProps.put("prop3", "Line3");
        newProps.put("prop4", "Line4");

        expResult.clear();
        expResult.put("newProps1", "Line1");
        expResult.put("newProps2", "Line2");
        expResult.put("prop3", "Line3");
        expResult.put("prop4", "Line4");

        instance.setProperties(newProps, true);

        result = instance.getProperties();
        assertEquals(expResult, result);
        instance.close();
    }

    @Test
    public void testGetProperty_UpdateAndPersistStrategy_Refresh_false() throws Exception {

        Reloadable instance = new FilePropertyManager(file.getAbsolutePath(), Reloadable.RELOAD_STRATEGY.NONE,
                Reloadable.UPDATE_STRATEGY.INTERNAL);

        Map<String, String> expResult = new HashMap<String, String>();
        expResult.put(KEY_1, VALUE_1);
        expResult.put(KEY_2, VALUE_2);

        Map<String, String> result = instance.getProperties();
        assertEquals(expResult, result);

        Map<String, String> newProps = new HashMap<String, String>();
        newProps.put(KEY_1, "Changed1");
        newProps.put("prop3", "Line3");
        newProps.put("prop4", "Line4");

        instance.setProperties(newProps, false);

        assertEquals("Changed1", instance.getProperty(KEY_1));
        assertEquals(VALUE_2, instance.getProperty(KEY_2));
        assertEquals("Line3", instance.getProperty("prop3"));
        instance.close();
    }

    @Test
    public void testGetProperties_UpdateAndPersistStrategy_Refresh_false() throws Exception {

        Reloadable instance = new FilePropertyManager(file.getAbsolutePath(), Reloadable.RELOAD_STRATEGY.NONE,
                Reloadable.UPDATE_STRATEGY.INTERNAL);

        Map<String, String> expResult = new HashMap<String, String>();
        expResult.put(KEY_1, VALUE_1);
        expResult.put(KEY_2, VALUE_2);

        Map<String, String> result = instance.getProperties();
        assertEquals(expResult, result);

        Map<String, String> newProps = new HashMap<String, String>();
        newProps.put(KEY_1, "Changed1");
        newProps.put(KEY_2, "Changed2");
        newProps.put("prop3", "Line3");
        newProps.put("prop4", "Line4");

        expResult.put(KEY_1, "Changed1");
        expResult.put(KEY_2, "Changed2");
        expResult.put("prop3", "Line3");
        expResult.put("prop4", "Line4");

        instance.setProperties(newProps, false);

        result = instance.getProperties();
        assertEquals(expResult, result);
        instance.close();
    }

    @Test
    public void testGetPropertiesExternalUpdateStrategy() throws Exception {

        Reloadable instance = new FilePropertyManager(file.getAbsolutePath(), Reloadable.RELOAD_STRATEGY.STORE_CHANGED,
                Reloadable.UPDATE_STRATEGY.EXTERNAL, TestUtils.RELOAD_INTERVAL_SEC, TimeUnit.SECONDS);

        Map<String, String> expResult = new HashMap<String, String>();
        expResult.put(KEY_1, VALUE_1);
        expResult.put(KEY_2, VALUE_2);

        Map<String, String> result = instance.getProperties();
        assertEquals(expResult, result);

        BufferedWriter writer = getWriterForFile();

        Properties prop = new Properties();
        prop.put(KEY_1, "Changed1");
        prop.put(KEY_2, "Changed2");
        prop.store(writer, null);

        deallocate(writer);

        expResult = new HashMap<String, String>();
        expResult.put(KEY_1, "Changed1");
        expResult.put(KEY_2, "Changed2");

        result = instance.getProperties();
        assertEquals(expResult, result);
        instance.close();
    }

    @Test
    public void testGetPropertyExternalUpdateStrategy_No_Reload() throws Exception {

        Reloadable instance = new FilePropertyManager(file.getAbsolutePath(), Reloadable.RELOAD_STRATEGY.NONE,
                Reloadable.UPDATE_STRATEGY.EXTERNAL);

        Map<String, String> expResult = new HashMap<String, String>();
        expResult.put(KEY_1, VALUE_1);
        expResult.put(KEY_2, VALUE_2);

        Map<String, String> result = instance.getProperties();
        assertEquals(expResult, result);

        BufferedWriter writer = getWriterForFile();

        Properties prop = new Properties();
        prop.put(KEY_1, "Changed1");
        prop.put(KEY_2, "Changed2");
        prop.store(writer, null);

        deallocate(writer);

        assertEquals(VALUE_1, instance.getProperty(KEY_1));
        instance.close();
    }

    @Test
    public void testGetPropertiesExternalUpdateStrategy_No_Reload() throws Exception {

        Reloadable instance = new FilePropertyManager(file.getAbsolutePath(), Reloadable.RELOAD_STRATEGY.NONE,
                Reloadable.UPDATE_STRATEGY.EXTERNAL);

        Map<String, String> expResult = new HashMap<String, String>();
        expResult.put(KEY_1, VALUE_1);
        expResult.put(KEY_2, VALUE_2);

        Map<String, String> result = instance.getProperties();
        assertEquals(expResult, result);

        BufferedWriter writer = getWriterForFile();

        Properties prop = new Properties();
        prop.put(KEY_1, "Changed1");
        prop.put(KEY_2, "Changed2");
        prop.store(writer, null);

        deallocate(writer);

        result = instance.getProperties();
        assertEquals(expResult, result);
        instance.close();
    }

    /**
     * Test of getProperty method, of class FileLoader.
     */
    @Test
    public void testGetPropertyForStoreChangeStrategy() throws Exception {

        Reloadable instance = new FilePropertyManager(file.getAbsolutePath(), Reloadable.RELOAD_STRATEGY.STORE_CHANGED,
                Reloadable.UPDATE_STRATEGY.EXTERNAL);
        String result = instance.getProperty(KEY_2);
        assertEquals(VALUE_2, result);
        instance.close();

    }

    /**
     * Test of getProperties method, of class FileLoader.
     */
    @Test
    public void testGetPropertiesForStoreChangeStrategy() throws Exception {

        Reloadable instance = new FilePropertyManager(file.getAbsolutePath(), Reloadable.RELOAD_STRATEGY.STORE_CHANGED,
                Reloadable.UPDATE_STRATEGY.EXTERNAL);

        Map<String, String> expResult = new HashMap<String, String>();
        expResult.put(KEY_1, VALUE_1);
        expResult.put(KEY_2, VALUE_2);

        Map<String, String> result = instance.getProperties();
        assertEquals(expResult, result);
        instance.close();
    }

    @Test
    public void testGetPropertyPropertyChangeForStoreChangeStrategy() throws Exception {

        BufferedWriter writer = getWriterForFile();

        Properties prop = new Properties();
        prop.put(KEY_1, "Changed1");
        prop.put(KEY_2, "Changed2");
        prop.store(writer, null);

        deallocate(writer);

        String expectedResult1 = "Changed1";
        String expectedResult2 = "Changed2";
        Reloadable instance = new FilePropertyManager(file.getAbsolutePath(), Reloadable.RELOAD_STRATEGY.STORE_CHANGED,
                Reloadable.UPDATE_STRATEGY.EXTERNAL);
        String result1 = instance.getProperty(KEY_1);
        String result2 = instance.getProperty(KEY_2);

        assertEquals(expectedResult1, result1);
        assertEquals(expectedResult2, result2);
        instance.close();

    }

    /**
     * Test of getProperties method, of class FileLoader.
     */
    @Test
    public void testGetPropertiesPropertyChangeForStoreChangeStrategy() throws Exception {

        Reloadable instance = new FilePropertyManager(file.getAbsolutePath(), Reloadable.RELOAD_STRATEGY.STORE_CHANGED,
                Reloadable.UPDATE_STRATEGY.EXTERNAL);

        Map<String, String> expResult = new HashMap<String, String>();
        expResult.put(KEY_1, VALUE_1);
        expResult.put(KEY_2, VALUE_2);

        Map<String, String> result = instance.getProperties();
        assertEquals(expResult, result);

        BufferedWriter writer = getWriterForFile();

        Properties prop = new Properties();
        prop.put(KEY_1, "Changed1");
        prop.put(KEY_2, "Changed2");
        prop.store(writer, null);

        deallocate(writer);

        // Thread.sleep(10000);
        expResult = new HashMap<String, String>();
        expResult.put(KEY_1, "Changed1");
        expResult.put(KEY_2, "Changed2");

        result = instance.getProperties();
        assertEquals(expResult, result);
        instance.close();
    }

    @Test
    public void testGetPropertyPropertyChangeForDefaultIntervalStrategy() throws Exception {

        String expectedResult1 = "Changed1";
        String expectedResult2 = "Changed2";
        Reloadable instance = new FilePropertyManager(file.getAbsolutePath(), Reloadable.RELOAD_STRATEGY.INTERVAL,
                Reloadable.UPDATE_STRATEGY.EXTERNAL);

        BufferedWriter writer = getWriterForFile();

        Properties prop = new Properties();
        prop.put(KEY_1, "Changed1");
        prop.put(KEY_2, "Changed2");
        prop.store(writer, null);

        deallocate(writer);

        TestUtils.maxWaitFor(70).untilAsserted(() -> {
            assertEquals(expectedResult1, instance.getProperty(KEY_1));
            assertEquals(expectedResult2, instance.getProperty(KEY_2));
        });

        instance.close();
    }

    @Test
    public void testGetPropertiesPropertyChangeForDefaultIntervalStrategy() throws Exception {

        Reloadable instance = new FilePropertyManager(file.getAbsolutePath(), Reloadable.RELOAD_STRATEGY.INTERVAL,
                Reloadable.UPDATE_STRATEGY.EXTERNAL);

        Map<String, String> expResult = new HashMap<String, String>();
        expResult.put(KEY_1, VALUE_1);
        expResult.put(KEY_2, VALUE_2);

        Map<String, String> result = instance.getProperties();
        assertEquals(expResult, result);

        BufferedWriter writer = getWriterForFile();

        Properties prop = new Properties();
        prop.put(KEY_1, "Changed1");
        prop.put(KEY_2, "Changed2");
        prop.store(writer, null);

        deallocate(writer);
        expResult.clear();
        expResult.put(KEY_1, "Changed1");
        expResult.put(KEY_2, "Changed2");

        TestUtils.maxWaitFor(70).untilAsserted(() -> assertEquals(expResult, instance.getProperties()));
        instance.close();
    }

    @Test
    public void testGetPropertyPropertyChangeForIntervalStrategy() throws Exception {

        String expectedResult1 = "Changed1";
        String expectedResult2 = "Changed2";
        Reloadable instance = new FilePropertyManager(file.getAbsolutePath(), Reloadable.RELOAD_STRATEGY.INTERVAL,
                Reloadable.UPDATE_STRATEGY.EXTERNAL, TestUtils.RELOAD_INTERVAL_SEC, TimeUnit.SECONDS);

        BufferedWriter writer = getWriterForFile();

        Properties prop = new Properties();
        prop.put(KEY_1, "Changed1");
        prop.put(KEY_2, "Changed2");
        prop.store(writer, null);

        deallocate(writer);

        TestUtils.maxWaitFor(130).untilAsserted(() -> {
            assertEquals(expectedResult1, instance.getProperty(KEY_1));
            assertEquals(expectedResult2, instance.getProperty(KEY_2));
        });

        instance.close();
    }

    @Test
    public void testGetPropertiesPropertyChangeForIntervalStrategy() throws Exception {

        Reloadable instance = new FilePropertyManager(file.getAbsolutePath(), Reloadable.RELOAD_STRATEGY.INTERVAL,
                Reloadable.UPDATE_STRATEGY.EXTERNAL, TestUtils.RELOAD_INTERVAL_SEC, TimeUnit.SECONDS);

        Map<String, String> expResult = new HashMap<String, String>();
        expResult.put(KEY_1, VALUE_1);
        expResult.put(KEY_2, VALUE_2);

        Map<String, String> result = instance.getProperties();
        assertEquals(expResult, result);

        BufferedWriter writer = getWriterForFile();

        Properties prop = new Properties();
        prop.put(KEY_1, "Changed1");
        prop.put(KEY_2, "Changed2");
        prop.store(writer, null);

        deallocate(writer);
        expResult.clear();
        expResult.put(KEY_1, "Changed1");
        expResult.put(KEY_2, "Changed2");

        TestUtils.maxWaitFor(130).untilAsserted(() -> assertEquals(expResult, instance.getProperties()));
        instance.close();
    }

    @Test
    public void testSetProperty_InternalStrategy() throws Exception {

        Reloadable instance = new FilePropertyManager(file.getAbsolutePath(), Reloadable.RELOAD_STRATEGY.NONE,
                Reloadable.UPDATE_STRATEGY.INTERNAL);

        Map<String, String> expResult = new HashMap<String, String>();
        expResult.put(KEY_1, VALUE_1);
        expResult.put(KEY_2, VALUE_2);

        Map<String, String> result = instance.getProperties();
        assertEquals(expResult, result);

        instance.setProperty(KEY_1, "Changed1");
        instance.setProperty("prop3", "Line3");

        assertEquals("Changed1", instance.getProperty(KEY_1));
        assertEquals(VALUE_2, instance.getProperty(KEY_2));
        assertEquals("Line3", instance.getProperty("prop3"));
        instance.close();
    }

    @Test
    public void testSetProperties_InternalStrategy_Refresh_true() throws Exception {

        Reloadable instance = new FilePropertyManager(file.getAbsolutePath(), Reloadable.RELOAD_STRATEGY.NONE,
                Reloadable.UPDATE_STRATEGY.INTERNAL);

        Map<String, String> expResult = new HashMap<String, String>();
        expResult.put(KEY_1, VALUE_1);
        expResult.put(KEY_2, VALUE_2);

        Map<String, String> result = instance.getProperties();
        assertEquals(expResult, result);

        Map<String, String> newProps = new HashMap<String, String>();
        newProps.put("newProps1", "Line1");
        newProps.put("newProps2", "Line2");
        newProps.put("prop3", "Line3");
        newProps.put("prop4", "Line4");

        expResult.clear();
        expResult.put("newProps1", "Line1");
        expResult.put("newProps2", "Line2");
        expResult.put("prop3", "Line3");
        expResult.put("prop4", "Line4");

        instance.setProperties(newProps, true);

        result = instance.getProperties();
        assertEquals(expResult, result);
        instance.close();
    }

    @Test
    public void testSetProperties_InternalStrategy_Refresh_false() throws Exception {

        Reloadable instance = new FilePropertyManager(file.getAbsolutePath(), Reloadable.RELOAD_STRATEGY.NONE,
                Reloadable.UPDATE_STRATEGY.INTERNAL);

        Map<String, String> expResult = new HashMap<String, String>();
        expResult.put(KEY_1, VALUE_1);
        expResult.put(KEY_2, VALUE_2);

        Map<String, String> result = instance.getProperties();
        assertEquals(expResult, result);

        Map<String, String> newProps = new HashMap<String, String>();
        newProps.put(KEY_1, "Changed1");
        newProps.put(KEY_2, "Changed2");
        newProps.put("prop3", "Line3");
        newProps.put("prop4", "Line4");

        expResult.put(KEY_1, "Changed1");
        expResult.put(KEY_2, "Changed2");
        expResult.put("prop3", "Line3");
        expResult.put("prop4", "Line4");

        instance.setProperties(newProps, false);

        result = instance.getProperties();
        assertEquals(expResult, result);
        instance.close();
    }

    /**
     * Test of getProperty method, of class FileLoader.
     */
    @Test
    public void testGetPropertyForIntervalStrategy() throws Exception {

        Reloadable instance = new FilePropertyManager(file.getAbsolutePath(), Reloadable.RELOAD_STRATEGY.INTERVAL,
                Reloadable.UPDATE_STRATEGY.EXTERNAL);
        String result = instance.getProperty(KEY_2);
        assertEquals(VALUE_2, result);
        instance.close();

    }

    @Test
    public void testInvalidPropertyForStoreChangeStrategy() throws Exception {

        Reloadable instance = new FilePropertyManager(file.getAbsolutePath(), Reloadable.RELOAD_STRATEGY.STORE_CHANGED,
                Reloadable.UPDATE_STRATEGY.EXTERNAL);
        String result = instance.getProperty("INVALID");
        assertNull(result);
        instance.close();

    }

    @Test
    public void testInvalidPropertyForIntervalStrategy() throws Exception {

        Reloadable instance = new FilePropertyManager(file.getAbsolutePath(), Reloadable.RELOAD_STRATEGY.INTERVAL,
                Reloadable.UPDATE_STRATEGY.EXTERNAL);
        String result = instance.getProperty("INVALID");
        assertNull(result);
        instance.close();

    }

    @Test
    public void testSetProperties_External_Update() throws Exception {

        Reloadable instance = new FilePropertyManager(file.getAbsolutePath(), Reloadable.RELOAD_STRATEGY.STORE_CHANGED,
                Reloadable.UPDATE_STRATEGY.EXTERNAL);

       
        String newProp = "newProps1";
        try (FileOutputStream outStream = new FileOutputStream(file)) {

            Map<String, String> newProps = new HashMap<String, String>();
            newProps.put(newProp, "Line1");

            instance.setProperties(newProps, true);

        }

        assertNull(instance.getProperty(newProp));

    }

    @Test
    public void testNoFileForStoreChangeStrategy() throws Exception {
        assertThrows(PropertyException.class, () -> new FilePropertyManager("invalid.txt",
                Reloadable.RELOAD_STRATEGY.STORE_CHANGED, Reloadable.UPDATE_STRATEGY.EXTERNAL));

    }

    @Test
    public void testNoFileForIntervalStrategy() throws Exception {
        assertThrows(PropertyException.class, () -> new FilePropertyManager("invalid.txt",
                Reloadable.RELOAD_STRATEGY.INTERVAL, Reloadable.UPDATE_STRATEGY.EXTERNAL));

    }

    @Test
    public void testInvalidReloadInterval() throws Exception {
        assertThrows(PropertyException.class, () -> new FilePropertyManager(file.getAbsolutePath(),
                Reloadable.RELOAD_STRATEGY.INTERVAL, Reloadable.UPDATE_STRATEGY.EXTERNAL, 0, TimeUnit.SECONDS));
    }

    private BufferedWriter getWriterForFile() throws Exception {
        return new BufferedWriter(new FileWriter(file));
    }

    private void deallocate(Writer writer) throws Exception {
        writer.flush();
        writer.close();
    }

}