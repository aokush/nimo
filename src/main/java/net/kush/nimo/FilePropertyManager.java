package net.kush.nimo;

import it.sauronsoftware.cron4j.Scheduler;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
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
    private Scheduler scheduler;
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private long lastModified;
    private static final String FILE_NOT_FOUND_TMPLT = "'%s' does not exist";
    private String scheduleId;

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
        this(filePath, strategy, updateStrategy, 1);
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
     * @param interval       An integer representing interval in minutes between
     *                       file reloads. Value must be 1 and above.
     * @throws PropertyException If filePath does not refer to an existing file or
     *                           if interval is less than one(1)
     */
    public FilePropertyManager(String filePath, RELOAD_STRATEGY strategy, UPDATE_STRATEGY updateStrategy, int interval)
            throws PropertyException {
        File file = new File(filePath);

        if (!file.exists() || !file.isFile()) {
            throw new PropertyException(String.format(FILE_NOT_FOUND_TMPLT, filePath));
        }
        this.filePath = filePath;
        this.strategy = strategy;
        this.updateStrategy = updateStrategy;

        if (RELOAD_STRATEGY.INTERVAL.equals(strategy)) {

            if (interval < 1) {
                throw new PropertyException(String.format("interval must be a number greater than '%s'", 0));
            }

            StringBuilder schedulePattern = new StringBuilder();
            schedulePattern.append("*/").append(interval).append(" * * * *");

            scheduler = Util.getOrCreateScheduler();
            FileTask task = new FileTask();
            // Make sure file is loaded before actual scheduling begins
            task.run();

            scheduleId = scheduler.schedule(schedulePattern.toString(), task);

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
        } catch (FileNotFoundException nfe) {
            throw new PropertyException(String.format(FILE_NOT_FOUND_TMPLT, filePath));
        } catch (IOException io) {
            throw new PropertyException(String.format("Error reading file '%s'", filePath));
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
        } catch (FileNotFoundException nfe) {
            throw new PropertyException(String.format(FILE_NOT_FOUND_TMPLT, filePath));
        } catch (IOException io) {
            throw new PropertyException(String.format("Error reading file '%s'", filePath));
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
        Util.deschedule(scheduleId);
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

        @Override
        public void run() {
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
