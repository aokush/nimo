package net.kush.nimo;

import it.sauronsoftware.cron4j.Scheduler;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
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

    private final String selectQueryTemplate;
    private final String insertQueryTemplate;
    private final String updateQueryTemplate;
    private String tableName;

    private Reloadable.RELOAD_STRATEGY reloadStrategy;
    private Reloadable.UPDATE_STRATEGY updateStrategy;
    private Map<String, String> propertiesMap = new HashMap<>();

    private Scheduler scheduler;
    private String scheduleId;
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
    public SQLDatabasePropertyManager(String tableName, String keyColumnName, String valueColumnName, DataSource ds)
            throws PropertyException {
        this(tableName, keyColumnName, valueColumnName, ds, Reloadable.RELOAD_STRATEGY.NONE,
                Reloadable.UPDATE_STRATEGY.INTERNAL, 1);
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
     * @param interval        An integer representing interval in minutes between
     *                        properties reloads. Value must be 1 and above.
     * @throws PropertyException If selectQuery is null or invalid sql query or if
     *                           interval is less than one(1)
     */
    public SQLDatabasePropertyManager(String tableName, String keyColumnName, String valueColumnName, DataSource ds,
            Reloadable.RELOAD_STRATEGY reloadStrategy, Reloadable.UPDATE_STRATEGY updateStrategy, int interval)
            throws PropertyException {
        this.ds = ds;
        this.tableName = tableName;
        this.reloadStrategy = reloadStrategy;
        this.updateStrategy = updateStrategy;

        selectQueryTemplate = "SELECT " + keyColumnName + ", " + valueColumnName + " FROM " + tableName;
        insertQueryTemplate = "INSERT INTO " + tableName + " VALUES('%s', '%s')";
        updateQueryTemplate = "UPDATE " + tableName + " SET " + valueColumnName + "= '%s' WHERE " + keyColumnName
                + " = '%s'";

        // check select query is valid
        if (tableName == null || tableName.isEmpty() || keyColumnName == null || keyColumnName.isEmpty()
                || valueColumnName == null || valueColumnName.isEmpty()) {

            throw new PropertyException(
                    "Please provide a valid 'tableName', 'keyColumnName' and 'valueColumnName' cannot be null or empty");

        }

        if (reloadStrategy == null) {
            throw new PropertyException("Inavlid 'reloadStrategy'");
        }

        if (updateStrategy == null) {
            throw new PropertyException("Inavlid 'updateStrategy'");
        }

        if (RELOAD_STRATEGY.STORE_CHANGED.equals(reloadStrategy)) {
            logger.log(Level.WARNING, "'Reloadable.RELOAD_STRATEGY.STORE_CHANGED' may lead to poor performance");
        }

        Connection conn = null;
        try {
            conn = ds.getConnection();
        } catch (SQLException sqle) {
            logger.log(Level.SEVERE, "Cannot connection to database", sqle);
            throw new PropertyException(sqle);
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException sqle) {
                logger.log(Level.WARNING, "Cannot close connection to database");
            }
        }

        try {
            loadProperties();
        } catch (SQLException sqe) {
            throw new PropertyException(sqe);
        }

        if (RELOAD_STRATEGY.INTERVAL.equals(this.reloadStrategy)) {

            if (interval < 1) {
                throw new PropertyException(String.format("interval must be a number greater than '%s'", 0));
            }

            StringBuilder schedulePattern = new StringBuilder();
            schedulePattern.append("*/").append(interval).append(" * * * *");

            scheduler = Util.getOrCreateScheduler();
            SQLDatabasePropertyManager.ReadTask task = new SQLDatabasePropertyManager.ReadTask();

            scheduleId = scheduler.schedule(schedulePattern.toString(), task);

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
            lock.lock();
            value = propertiesMap.get(key);
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

        Map<String, String> propertiesMapCopy = new HashMap<String, String>();
        Lock lock = lockMaker.readLock();
        try {
            lock.lock();
            propertiesMapCopy.putAll(propertiesMap);
        } finally {
            lock.unlock();
        }

        return propertiesMapCopy;
    }

    @Override
    public void close() {
        Util.deschedule(scheduleId);
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

                stmt.addBatch(String.format("DELETE FROM %s", tableName));

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
