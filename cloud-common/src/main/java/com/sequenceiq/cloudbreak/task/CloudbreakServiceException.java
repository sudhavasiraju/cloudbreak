package com.sequenceiq.cloudbreak.task;


public class CloudbreakServiceException extends RuntimeException {
    public CloudbreakServiceException(String message) {
        super(message);
    }

    public CloudbreakServiceException(String message, Throwable cause) {
        super(message, cause);
    }

    public CloudbreakServiceException(Throwable cause) {
        super(cause);
    }
}
