package com.sequenceiq.cloudbreak.cluster.ambari.task;

import java.util.List;

import com.sequenceiq.ambari.client.AmbariClient;
import com.sequenceiq.cloudbreak.domain.Stack;
import com.sequenceiq.cloudbreak.context.StackContext;

public class AmbariStartupPollerObject extends StackContext {

    private String ambariIp;

    private List<AmbariClient> ambariClients;

    public AmbariStartupPollerObject(Stack stack, String ambariIp, List<AmbariClient> ambariClients) {
        super(stack);
        this.ambariIp = ambariIp;
        this.ambariClients = ambariClients;
    }

    public String getAmbariAddress() {
        return ambariIp;
    }

    public void setAmbariIp(String ambariIp) {
        this.ambariIp = ambariIp;
    }

    public List<AmbariClient> getAmbariClients() {
        return ambariClients;
    }

    public void setAmbariClients(List<AmbariClient> ambariClient) {
        ambariClients = ambariClient;
    }
}
