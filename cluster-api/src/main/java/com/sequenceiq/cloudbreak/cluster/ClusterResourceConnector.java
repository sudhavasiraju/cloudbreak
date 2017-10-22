package com.sequenceiq.cloudbreak.cluster;

public interface ClusterResourceConnector {

    void waitForApi();

    void waitForHosts();

    void waitClusterService();

    boolean isApiAvailable();

    void buildCluster();

    void installCluster();

    void updateAuthentication();

    void stopCluster();

    void startCluster();
}
