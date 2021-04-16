package net.kush.nimo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.derby.jdbc.EmbeddedConnectionPoolDataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 *
 * @author Home
 */
public class SQLDatabasePropertyManagerTest {

    // private static NetworkServerControl dbServer;
    private static DataSource ds;

    private static final String TABLE_NAME = "KEYVALUE";
    private static final String TABLE_COL_KEY = "ID";
    private static final String TABLE_COL_VALUE = "VALUE";

    // private static final String TABLE_SELECT_ALL_QRY = "SELECT * FROM %s";
    // private static final String TABLE_SELECT_COL_QRY = "SELECT %s FROM %s";
    private static final String TABLE_INSERT_QRY = "INSERT INTO %s VALUES ('%s', '%s')";
    private static final String TABLE_UPDATE_QRY = "UPDATE %s SET %s = '%s' WHERE %s = '%s'";

    private static final String KEY_1 = "prop1";
    private static final String KEY_2 = "prop2";
    private static final String KEY_3 = "prop3";
    private static final String VALUE_1 = "Line1";
    private static final String VALUE_2 = "Line2";
    private static final String VALUE_3 = "Line3";
    private static final String VALUE_1_CHANGED = "Line1-Changed";
    private static final String VALUE_2_CHANGED = "Line2-Changed";

    @BeforeAll
    public static void setUpClass() throws Exception {

        EmbeddedConnectionPoolDataSource eds = new EmbeddedConnectionPoolDataSource();

        eds.setConnectionAttributes("create=true");
        eds.setDatabaseName("nimodb");

        ds = eds;
    }

    @AfterAll
    public static void tearDownClass() throws Exception {
        File file = new File("nimodb");
        file.delete();
    }

    @BeforeEach
    public void setUp() throws SQLException {
        Connection conn = ds.getConnection();

        Statement stmt = conn.createStatement();

        stmt.executeUpdate(String.format("CREATE TABLE %s (%s varchar(40), %s varchar(40))", TABLE_NAME, TABLE_COL_KEY,
                TABLE_COL_VALUE));
        stmt.addBatch(String.format(TABLE_INSERT_QRY, TABLE_NAME, KEY_1, VALUE_1));
        stmt.addBatch(String.format(TABLE_INSERT_QRY, TABLE_NAME, KEY_2, VALUE_2));
        stmt.executeBatch();

        stmt.close();
        conn.close();
    }

    @AfterEach
    public void tearDown() throws SQLException {
        Connection conn = ds.getConnection();

        Statement stmt = conn.createStatement();
        stmt.execute(String.format("DROP TABLE %s", TABLE_NAME));

        stmt.close();
        conn.close();
    }

    /**
     * Test of getProperty method, of class SQLDatabaseLoader.
     */
    @Test
    public void testGetProperty() throws Exception {
        SQLDatabasePropertyManager instance = new SQLDatabasePropertyManager(TABLE_NAME, TABLE_COL_KEY, TABLE_COL_VALUE,
                ds);
        String result = instance.getProperty(KEY_2);
        instance.close();
        assertEquals(VALUE_2, result);
    }

    /**
     * Test of getProperties method, of class SQLDatabaseLoader.
     */
    @Test
    public void testGetProperties() throws Exception {
        SQLDatabasePropertyManager instance = new SQLDatabasePropertyManager(TABLE_NAME, TABLE_COL_KEY, TABLE_COL_VALUE,
                ds);

        Map<String, String> expResult = new HashMap<String, String>();
        expResult.put(KEY_1, VALUE_1);
        expResult.put(KEY_2, VALUE_2);

        Map<String, String> result = instance.getProperties();
        assertEquals(expResult, result);
        instance.close();
    }

    @Test
    public void testGetPropertyForIntervalReload() throws Exception {
        SQLDatabasePropertyManager instance = new SQLDatabasePropertyManager(TABLE_NAME, TABLE_COL_KEY, TABLE_COL_VALUE,
                ds, Reloadable.RELOAD_STRATEGY.INTERVAL, Reloadable.UPDATE_STRATEGY.EXTERNAL, TestUtils.DEFAULT_RELOAD_INTERVAL);

        try (Connection conn = ds.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(String.format(TABLE_UPDATE_QRY, TABLE_NAME, TABLE_COL_VALUE, VALUE_2_CHANGED,
                    TABLE_COL_KEY, KEY_2));
        }

        TestUtils.maxWaitFor(130).untilAsserted(() -> assertEquals(VALUE_2_CHANGED, instance.getProperty(KEY_2)));
        instance.close();
    }

    @Test
    public void testGetProperty_Store_Changed() throws Exception {
        SQLDatabasePropertyManager instance = new SQLDatabasePropertyManager(TABLE_NAME, TABLE_COL_KEY, TABLE_COL_VALUE,
                ds, Reloadable.RELOAD_STRATEGY.STORE_CHANGED, Reloadable.UPDATE_STRATEGY.EXTERNAL, TestUtils.DEFAULT_RELOAD_INTERVAL);

        try (Connection conn = ds.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(String.format(TABLE_UPDATE_QRY, TABLE_NAME, TABLE_COL_VALUE, VALUE_2_CHANGED,
                    TABLE_COL_KEY, KEY_2));
        }

        //TestUtils.maxWaitFor(70).untilAsserted(() -> assertEquals(VALUE_2_CHANGED, instance.getProperty(KEY_2)));
        assertEquals(VALUE_2_CHANGED, instance.getProperty(KEY_2));
        instance.close();
    }

    @Test
    public void testGetPropertiesForIntervalReload() throws Exception {
        SQLDatabasePropertyManager instance = new SQLDatabasePropertyManager(TABLE_NAME, TABLE_COL_KEY, TABLE_COL_VALUE,
                ds, Reloadable.RELOAD_STRATEGY.INTERVAL, Reloadable.UPDATE_STRATEGY.EXTERNAL, TestUtils.DEFAULT_RELOAD_INTERVAL);

        Map<String, String> expResult = new HashMap<String, String>();
        expResult.put(KEY_1, VALUE_1);
        expResult.put(KEY_2, VALUE_2);

        Map<String, String> result = instance.getProperties();
        assertEquals(expResult, result);

        try (Connection conn = ds.getConnection(); Statement stmt = conn.createStatement()) {

            stmt.addBatch(String.format(TABLE_INSERT_QRY, TABLE_NAME, KEY_3, VALUE_3));
            stmt.addBatch(String.format(TABLE_UPDATE_QRY, TABLE_NAME, TABLE_COL_VALUE, VALUE_1_CHANGED, TABLE_COL_KEY,
                    KEY_1));
            stmt.addBatch(String.format(TABLE_UPDATE_QRY, TABLE_NAME, TABLE_COL_VALUE, VALUE_2_CHANGED, TABLE_COL_KEY,
                    KEY_2));
            stmt.executeBatch();

            expResult.clear();
            expResult.put(KEY_1, VALUE_1_CHANGED);
            expResult.put(KEY_2, VALUE_2_CHANGED);
            expResult.put(KEY_3, VALUE_3);

            TestUtils.maxWaitFor(130).untilAsserted(() -> assertEquals(expResult, instance.getProperties()));
            instance.close();

        }
    }

    @Test
    public void testSetPropertyExisting() throws Exception {
        SQLDatabasePropertyManager instance = new SQLDatabasePropertyManager(TABLE_NAME, TABLE_COL_KEY, TABLE_COL_VALUE,
                ds);

        instance.setProperty(KEY_2, VALUE_2_CHANGED);
        assertEquals(VALUE_2_CHANGED, instance.getProperty(KEY_2));
    }

    @Test
    public void testSetPropertyNew() throws Exception {
        SQLDatabasePropertyManager instance = new SQLDatabasePropertyManager(TABLE_NAME, TABLE_COL_KEY, TABLE_COL_VALUE,
                ds);

        instance.setProperty("prop3", "line3");
        assertEquals("line3", instance.getProperty("prop3"));
    }

    @Test
    public void testSetProperties_refresh_true() throws Exception {
        SQLDatabasePropertyManager instance = new SQLDatabasePropertyManager(TABLE_NAME, TABLE_COL_KEY, TABLE_COL_VALUE,
                ds);

        Map<String, String> expResult = new HashMap<String, String>();
        expResult.put(KEY_1, VALUE_1);
        expResult.put(KEY_2, VALUE_2);

        assertEquals(expResult, instance.getProperties());

        expResult.clear();
        expResult.put("prop1_changed", VALUE_1_CHANGED);
        expResult.put("prop2_changed", VALUE_2_CHANGED);
        expResult.put(KEY_3, VALUE_3);

        instance.setProperties(expResult, true);

        assertEquals(expResult, instance.getProperties());
        instance.close();
    }

    @Test
    public void testSetProperties_refresh_false() throws Exception {
        SQLDatabasePropertyManager instance = new SQLDatabasePropertyManager(TABLE_NAME, TABLE_COL_KEY, TABLE_COL_VALUE,
                ds);

        Map<String, String> expResult = new HashMap<String, String>();
        expResult.put(KEY_1, VALUE_1);
        expResult.put(KEY_2, VALUE_2);

        assertEquals(expResult, instance.getProperties());

        expResult.clear();
        expResult.put(KEY_1, VALUE_1_CHANGED);
        expResult.put(KEY_2, VALUE_2_CHANGED);
        expResult.put(KEY_3, VALUE_3);

        instance.setProperties(expResult, false);

        assertEquals(expResult, instance.getProperties());
        instance.close();
    }

    @Test
    public void testSetProperty_External_Update() throws Exception {

        Reloadable instance = new SQLDatabasePropertyManager(TABLE_NAME, TABLE_COL_KEY, TABLE_COL_VALUE, ds,
                Reloadable.RELOAD_STRATEGY.INTERVAL, Reloadable.UPDATE_STRATEGY.EXTERNAL, TestUtils.DEFAULT_RELOAD_INTERVAL);

        String newProp = "newProps1";
        instance.setProperty(newProp, "Line1");

        assertNull(instance.getProperty(newProp));

    }

    @Test
    public void testSetProperties_External_Update() throws Exception {

        Reloadable instance = new SQLDatabasePropertyManager(TABLE_NAME, TABLE_COL_KEY, TABLE_COL_VALUE, ds,
                Reloadable.RELOAD_STRATEGY.INTERVAL, Reloadable.UPDATE_STRATEGY.EXTERNAL, TestUtils.DEFAULT_RELOAD_INTERVAL);

        String newProp = "newProps1";
        Map<String, String> newProps = new HashMap<String, String>();
        newProps.put(newProp, "Line1");

        instance.setProperties(newProps, true);

        assertNull(instance.getProperty(newProp));
        instance.close();

    }

    @Test
    public void testNullTableName() throws Exception {
        assertThrows(PropertyException.class,
                () -> new SQLDatabasePropertyManager(null, TABLE_COL_KEY, TABLE_COL_VALUE, ds));
    }

    @Test
    public void testNullColumnKeyName() throws Exception {
        assertThrows(PropertyException.class,
                () -> new SQLDatabasePropertyManager(TABLE_NAME, null, TABLE_COL_VALUE, ds));
    }

    @Test
    public void testNullColumnValueName() throws Exception {
        assertThrows(PropertyException.class,
                () -> new SQLDatabasePropertyManager(TABLE_NAME, TABLE_COL_KEY, null, ds));
    }

    @Test
    public void testNullDataSource() throws Exception {

        assertThrows(PropertyException.class,
                () -> new SQLDatabasePropertyManager(TABLE_NAME, TABLE_COL_KEY, TABLE_COL_VALUE, null));
    }

    @Test
    public void testInvalidReloadCronExpression() throws Exception {

        assertThrows(PropertyException.class, () -> new SQLDatabasePropertyManager(TABLE_NAME, TABLE_COL_KEY,
                TABLE_COL_VALUE, ds, Reloadable.RELOAD_STRATEGY.INTERVAL, Reloadable.UPDATE_STRATEGY.EXTERNAL, "gsgs"));
    }

    @Test
    public void testNullReloadStrategy() throws Exception {

        assertThrows(PropertyException.class, () -> new SQLDatabasePropertyManager(TABLE_NAME, TABLE_COL_KEY,
                TABLE_COL_VALUE, ds, null, Reloadable.UPDATE_STRATEGY.EXTERNAL, TestUtils.DEFAULT_RELOAD_INTERVAL));
    }

    @Test
    public void testNullUpdateStrategy() throws Exception {

        assertThrows(PropertyException.class, () -> new SQLDatabasePropertyManager(TABLE_NAME, TABLE_COL_KEY,
                TABLE_COL_VALUE, ds, Reloadable.RELOAD_STRATEGY.INTERVAL, null, TestUtils.DEFAULT_RELOAD_INTERVAL));
    }
}