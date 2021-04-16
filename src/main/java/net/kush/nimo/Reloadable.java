package net.kush.nimo;

import java.util.Map;

/**
 * Denotes a property source that detects changes and updates itself
 * automatically.
 *
 * @author Adebiyi Kuseju (Kush)
 */
public interface Reloadable {

    public static final String DEFAULT_RELOAD_INTERVAL ="*/1 * * * *";

    /**
     * A reload strategy to use for reloading the file containing the properties
     * configuration
     */
    public enum RELOAD_STRATEGY {
        NONE, INTERVAL, STORE_CHANGED
    };

    /**
     * Determines the approach to use when updating properties
     *
     * UPDATE assumes properties are updated externally outside Nimo
     * UPDATE_AND_PERSIST The in-memory property is updated first before
     * persisting into permanent storage PERSIST_AND_UPDATE The persistent
     * storage is updated first and then in-memory is updated by reading
     * persistent storage
     */
    public enum UPDATE_STRATEGY {

        EXTERNAL, INTERNAL
    };

    /**
     * Fetches the value of a property
     *
     * @param key A string identifier for the property to retrieve
     * @return A string that is a value of the key provided
     * @throws PropertyException If an error occurs when attempting to retrieve
     * the value of the specified key
     */
    String getProperty(String key) throws PropertyException;

    /**
     * Fetches the values of all the properties available in this loader
     *
     * @return A Map containing all the properties and their values
     * @throws PropertyException If an error occurs when attempting to retrieve
     * the available properties
     */
    Map<String, String> getProperties() throws PropertyException;

    /**
     * Add/updates the value of a property
     *
     * @param key A string identifier for the property to store
     * @param value A string the value the key points to
     * 
     * @throws PropertyException If an error occurs when attempting to store
     * the value of the specified key
     */
    void setProperty(String key, String value) throws PropertyException;

    /**
     * Add/updates the properties provided
     *
     * @param A Map containing all the properties and their values
     * @param refresh A boolean. True means discard all existing properties
     *                and replace with the new properties provided.
     *                false value means replace any matching property keys and add
     *                any non-existing ones in the properties map argument
     *
     * @throws PropertyException If an error occurs when attempting to store
     * the value of the specified key
     */
    void setProperties(Map<String, String> properties, boolean refresh) throws PropertyException;

    void close();
    
}
