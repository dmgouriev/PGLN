package ru.mvnm.pgln;

public class PglnConfigurationException extends Exception {

    public PglnConfigurationException(ConfigExceptionCallback callback) {
        super(callback.getErrorMessage());
    }

    interface ConfigExceptionCallback {
        String getErrorMessage();
    }
}
