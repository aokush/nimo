package net.kush.nimo;

/**
 * PropertyException
 *
 * @author Adebiyi Kuseju (Kush)
 */
public class PropertyException extends Exception {

    /**
     * Creates an instance of a PropertyException.
     *
     */
    public PropertyException() {
    }

    /**
     * Creates an instance of a PropertyException with a specific exception
     * message.
     *
     * @param msg
     */
    public PropertyException(String msg) {
        super(msg);
    }

    /**
     * Creates an instance of a PropertyException with a parent exception
     * @param msg
     * @param t
     */
    public PropertyException(String msg,Throwable t) {
        super(msg, t);
    }

    /**
     * Creates an instance of a PropertyException with a parent exception
     *
     * @param t
     */
    public PropertyException(Throwable t) {
        super(t);
    }

}
