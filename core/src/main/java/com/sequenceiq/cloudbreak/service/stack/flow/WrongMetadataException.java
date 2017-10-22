package com.sequenceiq.cloudbreak.service.stack.flow;

import com.sequenceiq.cloudbreak.task.CloudbreakServiceException;

public class WrongMetadataException extends CloudbreakServiceException {

    public WrongMetadataException(String message) {
        super(message);
    }

}
