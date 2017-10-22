package com.sequenceiq.cloudbreak.controller;

import com.sequenceiq.cloudbreak.CloudbreakApiException;

public class FlowsAlreadyRunningException extends CloudbreakApiException {

    public FlowsAlreadyRunningException(String message) {
        super(message);
    }

    public FlowsAlreadyRunningException(String message, Throwable cause) {
        super(message, cause);
    }
}
