package com.sequenceiq.cloudbreak.cluster.ambari.task;

import com.sequenceiq.cloudbreak.task.CloudbreakServiceException;

public class AmbariHostsUnavailableException extends CloudbreakServiceException {

    public AmbariHostsUnavailableException(String message) {
        super(message);
    }

}
