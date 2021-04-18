package net.kush.nimo;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A loader for file based properties configuration
 *
 * @author Adebiyi Kuseju (Kush)
 */
public class FilePropertyManager implements Reloadable {

    private String filePath;
    private RELOAD_STRATEGY strategy;
    private UPDATE_STRATEGY updateStrategy;
    private Map<String, String> propertiesMap;
    private ScheduledExecutorService scheduler;
    private ScheduledFuture<?> scheduleFuture;
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private long lastModified;
    private static final String FILE_NOT_FOUND_TMPLT = "'%s' does not exist";
    private static final String FILE_READ_ERROR_TMPLT = "Error reading file '%s'";

    /**
     * Creates a new FileLoader instance.
     *
     * The constructor assumes property updates will performed only using this
     * instance. It is optimized for efficient property update and retrieval
     *
     * @param filePath The path to the file containing properties to be loaded.
     *
     * @throws PropertyException If filePath does not refer to an existing file
     */
    public FilePropertyManager(String filePath) throws PropertyException {
        this(filePath, RELOAD_STRATEGY.NONE, UPDATE_STRATEGY.INTERNAL);
    }

    /**
     * Creates a new FileLoader instance.
     * 
     * If Reloadable.UPDATE_STRATEGY.EXTERNAL update strategy is used, in order to
     * get updates from persistent storage, it is important to use a reload strategy
     * other than Reloadable.RELOAD_STRATEGY.NONE
     *
     * @param filePath       The path to the file containing properties to be
     *                       loaded.
     * @param strategy       The reload strategy to use. If strategy is
     *                       Reloadable.RELOAD_STRATEGY.INTERVAL, file is reloaded
     *                       every minute.
     * @param updateStrategy The update strategy to use in getting updates from from
     *                       permanent store
     * @throws PropertyException If filePath does not refer to an existing file
     */
    public FilePropertyManager(String filePath, RELOAD_STRATEGY strategy, UPDATE_STRATEGY updateStrategy)
            throws PropertyException {
        this(filePath, strategy, updateStrategy, DEFAULT_RELOAD_INTERVAL, DEFAULT_TIME_UNIT);
    }

    /**
     * Creates a new FileLoader instance.
     *
     * @param filePath       The path to the file containing properties to be
     *                       loaded.
     * @param strategy       The reload strategy to use. If strategy is
     *                       Reloadable.RELOAD_STRATEGY.STORE_CHANGED or
     *                       Reloadable.RELOAD_STRATEGY.NONE, interval is ignored.
     * @param updateStrategy The update strategy to use in getting updates from from
     *                       permanent store
     * @param interval       A long that determines how often property should be
     *                       reloaded. Value must be greater than zero Only
     *                       applicable if Reloadable.RELOAD_STRATEGY.INTERNAL.
     * @param timeUnit       The time unit for the reload interval
     * @throws PropertyException If filePath does not refer to an existing file or
     *                           interval is not a positive whole number.
     */
    public FilePropertyManager(String filePath, RELOAD_STRATEGY strategy, UPDATE_STRATEGY updateStrategy, long interval,
            TimeUnit timeUnit) throws PropertyException {
        validateArgs(filePath, strategy, updateStrategy, interval);

        this.filePath = filePath;
        this.strategy = strategy;
        this.updateStrategy = updateStrategy;

        if (RELOAD_STRATEGY.INTERVAL.equals(strategy)) {

            scheduler = Util.getOrCreateScheduler();
            FileTask task = new FileTask();
            scheduleFuture = scheduler.scheduleWithFixedDelay(task, 1, interval, timeUnit);

        } else {
            try {
                loadFile();
            } catch (Exception e) {
                throw new PropertyException(e);
            }
        }
    }

    /**
     *
     * @see Reloadable#setProperty(java.lang.String, java.lang.String)
     */
    @Override
    public void setProperty(String key, String value) throws PropertyException {

        if (UPDATE_STRATEGY.EXTERNAL.equals(updateStrategy)) {
            return;
        }

        lock.writeLock().lock();

        try {
            propertiesMap.put(key, value);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     *
     * @see Reloadable#getProperty(java.lang.String)
     */
    @Override
    public String getProperty(String key) throws PropertyException {
        String value = null;

        try {
            if (RELOAD_STRATEGY.STORE_CHANGED.equals(strategy) && UPDATE_STRATEGY.EXTERNAL.equals(updateStrategy)) {
                File file = new File(filePath);

                if (file.lastModified() > lastModified) {
                    try {
                        lock.writeLock().lock();
                        loadFile();
                        lastModified = file.lastModified();
                    } finally {
                        lock.writeLock().unlock();
                    }
                }
            }

            value = propertiesMap.get(key);
        } catch (IOException io) {
            throw new PropertyException(String.format(FILE_READ_ERROR_TMPLT, filePath));
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
        try {

            if (RELOAD_STRATEGY.STORE_CHANGED.equals(strategy) && UPDATE_STRATEGY.EXTERNAL.equals(updateStrategy)) {
                File file = new File(filePath);

                if (file.lastModified() > lastModified) {

                    try {
                        lock.writeLock().lock();
                        loadFile();
                        lastModified = file.lastModified();
                    } finally {
                        lock.writeLock().unlock();
                    }

                }
            }

            propertiesMapCopy.putAll(propertiesMap);
        } catch (IOException io) {
            throw new PropertyException(String.format(FILE_READ_ERROR_TMPLT, filePath));
        }

        return propertiesMapCopy;
    }

    /**
     *
     * @see Reloadable#setProperties(java.util.Map, boolean)
     */
    @Override
    public void setProperties(Map<String, String> properties, boolean refresh) throws PropertyException {

        if (UPDATE_STRATEGY.EXTERNAL.equals(updateStrategy)) {
            return;
        }

        lock.writeLock().lock();

        if (refresh) {
            propertiesMap.clear();
        }

        try {

            Set<Entry<String, String>> propSet = properties.entrySet();

            for (Entry<String, String> entry : propSet) {
                propertiesMap.put(entry.getKey(), entry.getValue());
            }
            writeToFile(properties, refresh);

        } catch (Exception e) {
            throw new PropertyException(e);
        } finally {
            lock.writeLock().unlock();
        }

    }

    @Override
    public void close() {
        Util.deschedule(scheduleFuture);
    }

    private void validateArgs(String filePath, Reloadable.RELOAD_STRATEGY reloadStrategy,
            Reloadable.UPDATE_STRATEGY updateStrategy, long interval) throws PropertyException {
        File file = new File(filePath);

        if (!file.exists() || !file.isFile()) {
            throw new PropertyException(String.format(FILE_NOT_FOUND_TMPLT, filePath));
        }

        Util.validateArgs(reloadStrategy, updateStrategy, interval);

    }

    private void loadFile() throws IOException {

        try (FileInputStream inStream = new FileInputStream(new File(filePath))) {

            Properties props = new Properties();
            props.load(inStream);

            if (propertiesMap == null) {
                propertiesMap = new HashMap<>();
            }

            Set<Entry<Object, Object>> propSet = props.entrySet();

            for (Entry<Object, Object> entry : propSet) {
                propertiesMap.put(entry.getKey().toString(), entry.getValue().toString());
            }

        }

    }

    private void writeToFile(Map<String, String> propertiesMap, boolean refresh) throws IOException {

        try (FileOutputStream outStream = new FileOutputStream(new File(filePath))) {

            Properties props = new Properties();

            if (refresh) {
                props.putAll(propertiesMap);
            } else {
                try (FileInputStream inStream = new FileInputStream(new File(filePath))) {

                    props.load(inStream);

                    Set<Entry<String, String>> propSet = propertiesMap.entrySet();

                    for (Entry<String, String> entry : propSet) {
                        props.setProperty(entry.getKey(), entry.getValue());
                    }

                }

            }

            props.store(outStream, "");

        }

    }

    class FileTask implements Runnable {

        FileTask() {
            init();
        }

        @Override
        public void run() {
            init();
        }

        void init() {
            try {
                lock.writeLock().lock();
                loadFile();
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                lock.writeLock().unlock();
            }
        }
    }
}
