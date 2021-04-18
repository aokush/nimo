package net.kush.nimo;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.sql.DataSource;

/**
 * A loader for database based properties configuration
 *
 * @author Adebiyi Kuseju (Kush)
 */
public class SQLDatabasePropertyManager implements Reloadable {

    private static final Logger logger = Logger.getLogger(SQLDatabasePropertyManager.class.getName());

    public static final String TABLE_NAME = "table-name";
    public static final String KEY_COL_NAME = "key-col-name";
    public static final String VALUE_COL_NAME = "value-col-name";

    private final String selectQueryTemplate;
    private final String insertQueryTemplate;
    private final String updateQueryTemplate;

    private final Map<String, String> dbTableProps;

    private Reloadable.RELOAD_STRATEGY reloadStrategy;
    private Reloadable.UPDATE_STRATEGY updateStrategy;
    private Map<String, String> propertiesMap = new HashMap<>();

    private ScheduledExecutorService scheduler;
    private ScheduledFuture<?> scheduleFuture;
    private ReadWriteLock lockMaker = new ReentrantReadWriteLock();
    private DataSource ds;

    /**
     * Creates a new DatabaseLoader instance.
     *
     * Data is reloaded from the specified datasource every minute.
     *
     * @param tableName       The table name where properties are stored.
     * @param keyColumnName   The column name that contains the keys.
     * @param valueColumnName The column name that contains the values.
     * @param ds              The datasource to run the query against.
     * @throws PropertyException If selectQuery is not a valid sql query for the
     *                           target database
     */
    public SQLDatabasePropertyManager(Map<String, String> dbTableProps, DataSource ds) throws PropertyException {
        this(dbTableProps, ds, Reloadable.RELOAD_STRATEGY.NONE, Reloadable.UPDATE_STRATEGY.INTERNAL,
                DEFAULT_RELOAD_INTERVAL, DEFAULT_TIME_UNIT);
    }

    /**
     * Creates a new DatabaseLoader instance.
     *
     * @param tableName       The table name where properties are stored.
     * @param keyColumnName   The column name that contains the keys.
     * @param valueColumnName The column name that contains the values.
     * @param ds              The datasource to run the query against.
     * @param reloadStrategy  The reload strategy to use. If strategy is
     *                        Reloadable.RELOAD_STRATEGY.INTERVAL, properties are
     *                        reloaded every minute. Note: Avoid using
     *                        Reloadable.RELOAD_STRATEGY.STORE_CHANGED as this may
     *                        lead a considerable performance loss.
     *
     *                        If Reloadable.UPDATE_STRATEGY.EXTERNAL update strategy
     *                        is used, in order to get updates from persistent
     *                        storage, it is important to use a reload strategy
     *                        other than Reloadable.RELOAD_STRATEGY.NONE
     *
     * @param updateStrategy  The update strategy to use in getting updates from
     *                        from database
     * @param interval        A long that determines how often property should be reloaded.
     *                        Value must be greater than zero
     *                        Only applicable if Reloadable.RELOAD_STRATEGY.INTERNAL.
     * @param timeUnit        The time unit for the reload interval
     * @throws PropertyException If selectQuery is null or invalid sql query or interval is not a
     *                           positive whole number.
     */
    public SQLDatabasePropertyManager(Map<String, String> dbTableProps, DataSource ds,
            Reloadable.RELOAD_STRATEGY reloadStrategy, Reloadable.UPDATE_STRATEGY updateStrategy, long interval,
            TimeUnit timeUnit) throws PropertyException {
        this.ds = ds;
        ;
        this.reloadStrategy = reloadStrategy;
        this.updateStrategy = updateStrategy;

        validateArgs(dbTableProps, ds, reloadStrategy, updateStrategy, interval);

        selectQueryTemplate = "SELECT " + dbTableProps.get(KEY_COL_NAME) + ", " + dbTableProps.get(VALUE_COL_NAME)
                + " FROM " + dbTableProps.get(TABLE_NAME);
        insertQueryTemplate = "INSERT INTO " + dbTableProps.get(TABLE_NAME) + " VALUES('%s', '%s')";
        updateQueryTemplate = "UPDATE " + dbTableProps.get(TABLE_NAME) + " SET " + dbTableProps.get(VALUE_COL_NAME)
                + "= '%s' WHERE " + dbTableProps.get(KEY_COL_NAME) + " = '%s'";

        this.dbTableProps = dbTableProps;

        try {
            loadProperties();
        } catch (SQLException sqe) {
            throw new PropertyException(sqe);
        }

        if (RELOAD_STRATEGY.INTERVAL.equals(this.reloadStrategy)) {

            scheduler = Util.getOrCreateScheduler();
            SQLDatabasePropertyManager.ReadTask task = new SQLDatabasePropertyManager.ReadTask();
            scheduleFuture = scheduler.scheduleWithFixedDelay(task, 1, interval, timeUnit);

        }

    }

    /**
     *
     * @see Reloadable#getProperty(java.lang.String)
     */
    @Override
    public String getProperty(String key) throws PropertyException {
        Lock lock = lockMaker.readLock();
        String value;
        try {

            // check to see store has changed
            if (RELOAD_STRATEGY.STORE_CHANGED.equals(this.reloadStrategy)) {
                loadProperties();
            }

            lock.lock();
            value = propertiesMap.get(key);
        } catch (SQLException sqle) {
            throw new PropertyException(sqle);
        } finally {
            lock.unlock();
        }
        return value;
    }

    /**
     *
     * @see Reloadable#getProperties()
     */
    @Override
    public Map<String, String> getProperties() throws PropertyException {

        Map<String, String> propertiesMapCopy = new HashMap<>();
        Lock lock = lockMaker.readLock();
        try {

            // check to see store has changed
            if (RELOAD_STRATEGY.STORE_CHANGED.equals(this.reloadStrategy)) {
                loadProperties();
            }
            lock.lock();
            propertiesMapCopy.putAll(propertiesMap);
        } catch (SQLException sqle) {
            throw new PropertyException(sqle);
        } finally {
            lock.unlock();
        }

        return propertiesMapCopy;
    }

    @Override
    public void close() {
        Util.deschedule(scheduleFuture);
    }

    private void validateArgs(Map<String, String> dbTableProps, DataSource ds,
            Reloadable.RELOAD_STRATEGY reloadStrategy, Reloadable.UPDATE_STRATEGY updateStrategy, long interval)
            throws PropertyException {


        String tableName = dbTableProps.get(TABLE_NAME);
        String keyColumnName = dbTableProps.get(KEY_COL_NAME);
        String valueColumnName = dbTableProps.get(VALUE_COL_NAME);

        // check select query is valid
        if (tableName == null || tableName.isEmpty() || keyColumnName == null || keyColumnName.isEmpty()
                || valueColumnName == null || valueColumnName.isEmpty()) {
            throw new PropertyException(String.format(
                    "Please provide a valid tabel properties -'%s', '%s' and '%s' cannot be null or empty", TABLE_NAME,
                    KEY_COL_NAME, VALUE_COL_NAME));
        }

        Util.validateArgs(reloadStrategy, updateStrategy, interval);

        if (RELOAD_STRATEGY.STORE_CHANGED.equals(reloadStrategy)) {
            logger.log(Level.WARNING, "'Reloadable.RELOAD_STRATEGY.STORE_CHANGED' may lead to poor performance");
        }

        try (Connection conn = ds.getConnection()) {
            logger.log(Level.FINEST, "Connection test succeeded");
        } catch (Exception sqle) {
            throw new PropertyException("Cannot connection to database", sqle);
        }

    }

    private void loadProperties() throws SQLException {

        Lock lock = null;

        try (Connection conn = ds.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(selectQueryTemplate)) {

            try (ResultSet rs = pstmt.executeQuery()) {

                Map<String, String> temp = new HashMap<>();

                while (rs.next()) {
                    temp.put(rs.getString(1), rs.getString(2));
                }

                lock = lockMaker.writeLock();
                lock.lock();

                propertiesMap.clear();
                propertiesMap.putAll(temp);
            }

        } finally {

            if (lock != null) {
                lock.unlock();
            }
        }

    }

    public void setProperty(String key, String value) throws PropertyException {

        if (UPDATE_STRATEGY.EXTERNAL.equals(updateStrategy)) {
            return;
        }

        lockMaker.writeLock().lock();
        propertiesMap.put(key, value);

        try {
            Map<String, String> temp = new HashMap<>();
            temp.put(key, value);
            writeToDB(temp, false);
        } catch (Exception e) {
            throw new PropertyException(e);
        } finally {
            lockMaker.writeLock().unlock();
        }

    }

    public void setProperties(Map<String, String> properties, boolean refresh) throws PropertyException {
        if (UPDATE_STRATEGY.EXTERNAL.equals(updateStrategy)) {
            return;
        }

        lockMaker.writeLock().lock();

        if (refresh) {
            propertiesMap.clear();
        }

        try {

            writeToDB(properties, refresh);

            Set<Entry<String, String>> propSet = properties.entrySet();

            for (Entry<String, String> entry : propSet) {
                propertiesMap.put(entry.getKey(), entry.getValue());
            }
        } catch (Exception e) {
            throw new PropertyException(e);
        } finally {
            lockMaker.writeLock().unlock();
        }
    }

    private void writeToDB(Map<String, String> propertiesMap, boolean refresh) throws SQLException {

        try (Connection conn = ds.getConnection(); Statement stmt = conn.createStatement()) {

            if (refresh) {

                stmt.addBatch(String.format("DELETE FROM %s", dbTableProps.get(TABLE_NAME)));

                Set<Entry<String, String>> propSet = propertiesMap.entrySet();

                for (Entry<String, String> entry : propSet) {
                    stmt.addBatch(String.format(insertQueryTemplate, entry.getKey(), entry.getValue()));
                    propertiesMap.put(entry.getKey(), entry.getValue());
                }

            } else {

                Set<Entry<String, String>> propSet = propertiesMap.entrySet();

                for (Entry<String, String> entry : propSet) {

                    if (this.propertiesMap.containsKey(entry.getKey())) {
                        stmt.addBatch(String.format(updateQueryTemplate, entry.getKey(), entry.getValue()));
                    } else {
                        stmt.addBatch(String.format(insertQueryTemplate, entry.getKey(), entry.getValue()));
                    }

                    propertiesMap.put(entry.getKey(), entry.getValue());

                }

            }

            stmt.executeBatch();
        }

    }

    class ReadTask implements Runnable {

        @Override
        public void run() {
            try {
                loadProperties();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
