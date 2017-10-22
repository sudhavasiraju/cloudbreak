package com.sequenceiq.cloudbreak.cluster;

import com.sequenceiq.cloudbreak.cluster.model.ClusterType;

/**
 * Defines the constants to identify a Cluster provider
 */
public interface ClusterPlatformAware {

    /**
     * Name of the Cluster provider
     *
     * @return platform
     */
    ClusterType clusterType();

}
