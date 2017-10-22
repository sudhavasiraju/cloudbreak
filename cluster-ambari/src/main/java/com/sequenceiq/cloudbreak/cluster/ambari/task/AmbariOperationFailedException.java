package com.sequenceiq.cloudbreak.cluster.ambari.task;

import com.sequenceiq.cloudbreak.task.CloudbreakServiceException;

public class AmbariOperationFailedException extends CloudbreakServiceException {

    public AmbariOperationFailedException(String message) {
        super(message);
    }

    public AmbariOperationFailedException(String message, Throwable cause) {
        super(message, cause);
    }

}
