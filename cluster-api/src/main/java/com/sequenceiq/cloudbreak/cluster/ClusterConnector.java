package com.sequenceiq.cloudbreak.cluster;

public interface ClusterConnector extends ClusterPlatformAware {

    /**
     * Cluster related connector to the {@link ClusterResourceConnector} object.
     *
     * @return the {@link ClusterResourceConnector} object
     */
    ClusterResourceConnector clusterResourceConnector();

}
