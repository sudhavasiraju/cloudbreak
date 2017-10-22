package com.sequenceiq.cloudbreak.cluster.ambari;

import org.springframework.stereotype.Service;

import com.sequenceiq.cloudbreak.cluster.ClusterResourceConnector;

@Service
public class AmbariClusterResourceConnector implements ClusterResourceConnector {


    @Override
    public void waitForApi() {

    }

    @Override
    public void waitForHosts() {

    }

    @Override
    public void waitClusterService() {

    }

    @Override
    public boolean isApiAvailable() {
        return false;
    }

    @Override
    public void buildCluster() {

    }

    @Override
    public void installCluster() {

    }

    @Override
    public void updateAuthentication() {

    }

    @Override
    public void stopCluster() {

    }

    @Override
    public void startCluster() {

    }


}
