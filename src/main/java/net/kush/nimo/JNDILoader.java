package net.kush.nimo;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.naming.Context;
import javax.naming.NamingException;

import it.sauronsoftware.cron4j.InvalidPatternException;
import it.sauronsoftware.cron4j.Scheduler;

/**
 * A loader for JNDI based properties configuration
 *
 * @author Adebiyi Kuseju (Kush)
 */
public class JNDILoader implements Reloadable {

    private String jndiName;
    private Context context;
    private Map<String, String> propertiesMap = new HashMap<>();

    private Scheduler scheduler;
    private String scheduleId;
    private ReadWriteLock lockMaker = new ReentrantReadWriteLock();

    /**
     * Creates a new JNDILoader instance.
     *
     * Data is reloaded from the specified naming context every minute.
     *
     * @param context A fully configured naming context to be used to query
     *                the naming sever where properties are stored under unique
     *                "jndiName"
     * @param jndiName A unique name identifying a property implementation
     *                 object in a naming server.
     * @throws PropertyException If context is null or jndiName is null or jndiName 
     *         does not refer to an existing file
     */
    public JNDILoader(Context context, String jndiName) throws PropertyException {
        this(context, jndiName, DEFAULT_RELOAD_INTERVAL);
    }

    /**
     * Creates a new JNDILoader instance.
     *
     * Data is reloaded from the specified naming context at every interval.
     *
     * @param context A fully configured naming context to be used to query
     *                the naming sever where properties are stored under unique
     *                "jndiName"
     * @param jndiName A unique name identifying a property implementation
     *                 object in a naming server.
      * @param cronExpression  A valid cron expression that is used for reloading configuration if
     *                        reload strategy is Reloadable.RELOAD_STRATEGY.INTERNAL.
     * @throws PropertyException If context is null or jndiName is null
     */
    public JNDILoader(Context context, String jndiName, String cronExpression) throws PropertyException {
        this.jndiName = jndiName;
        this.context = context;

        if (context == null) {
            throw new PropertyException("Please provide a valid connection context objec to a JNDI compliant naming server");
        }
        
        if (jndiName == null) {
            throw new PropertyException("Please provide a valid JNDI name for identifying resource");
        }

        try {
            loadProperties();
        } catch (NamingException sqe) {
            throw new PropertyException(sqe);
        }

        scheduler = Util.getOrCreateScheduler();
        JNDILoader.ReadTask task = new JNDILoader.ReadTask();
        try {
            scheduleId = scheduler.schedule(cronExpression, task);
        } catch (InvalidPatternException ipe) {
            throw new PropertyException(ipe);
        }

    }
    
    /**
     * 
     * @see   Reloadable#getProperty(java.lang.String) 
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
     * @see   Reloadable#getProperties() 
     */
    @Override
    public Map<String, String> getProperties() throws PropertyException {
        Map<String, String> propertiesMapCopy = new HashMap<>();
        Lock lock = lockMaker.readLock();
        try {
            lock.lock();
            propertiesMapCopy.putAll(propertiesMap);
        } finally {
            lock.unlock();
        }


        return propertiesMapCopy;
    }

    private void loadProperties() throws NamingException, PropertyException {

        Lock lock = null;

        try {

            Map<String, String> temp = convertBoundObjectToMap(context.lookup(jndiName));
            lock = lockMaker.writeLock();
            lock.lock();

            propertiesMap.clear();
            propertiesMap.putAll(temp);

        } catch (NamingException nme) {
            throw nme;
        } catch (IllegalArgumentException pe) {
            throw new PropertyException(String.format("'%s must be bound to a java.util.Properties or java.util.Map instance'", jndiName));
        } finally {
            if (lock != null) {
                lock.unlock();
            }
        }


    }

    private Map<String, String> convertBoundObjectToMap(Object object) {
        
        if (object == null || !(object instanceof Map || object instanceof Properties)) {
            throw new IllegalArgumentException();
        }
        
        Map<String, String> temp = null;
        if (object instanceof Map) {
            temp = (Map) object;
        } else if (object instanceof Properties) {
            Properties prop = (Properties) object;
            
            Iterator<String> itr = prop.stringPropertyNames().iterator();
            temp = new HashMap<>();
            
            String key;
            while (itr.hasNext()) {
                key = itr.next();
                temp.put(key, prop.getProperty(key));
            }
            
        }

        return temp;
    }

    public void setProperty(String key, String value) throws PropertyException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public void setProperties(Map<String, String> properties, boolean refresh) throws PropertyException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void close() {
        Util.deschedule(scheduleId);
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
