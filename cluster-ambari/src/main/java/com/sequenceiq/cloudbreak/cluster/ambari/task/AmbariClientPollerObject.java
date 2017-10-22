package com.sequenceiq.cloudbreak.cluster.ambari.task;

import com.sequenceiq.ambari.client.AmbariClient;
import com.sequenceiq.cloudbreak.context.StackContext;
import com.sequenceiq.cloudbreak.domain.Stack;

public class AmbariClientPollerObject extends StackContext {

    private final AmbariClient ambariClient;

    public AmbariClientPollerObject(Stack stack, AmbariClient ambariClient) {
        super(stack);
        this.ambariClient = ambariClient;
    }

    public AmbariClient getAmbariClient() {
        return ambariClient;
    }
}
