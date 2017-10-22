package com.sequenceiq.cloudbreak.service.subscription;

import com.sequenceiq.cloudbreak.task.CloudbreakServiceException;

public class SubscriptionAlreadyExistException extends CloudbreakServiceException {

    public SubscriptionAlreadyExistException(String message) {
        super(message);
    }

}
