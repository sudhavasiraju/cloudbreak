package com.sequenceiq.cloudbreak;

public class CloudbreakImageNotFoundException extends Exception {

    public CloudbreakImageNotFoundException(String message) {
        super(message);
    }

    public CloudbreakImageNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public CloudbreakImageNotFoundException(Throwable cause) {
        super(cause);
    }
}
