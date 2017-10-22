package com.sequenceiq.cloudbreak.cluster.ambari;

import javax.inject.Inject;

import com.sequenceiq.cloudbreak.cluster.ClusterConnector;
import com.sequenceiq.cloudbreak.cluster.ClusterResourceConnector;
import com.sequenceiq.cloudbreak.cluster.model.ClusterType;

public class AmbariConnector implements ClusterConnector {

    @Inject
    private AmbariClusterResourceConnector ambariClusterResourceConnector;

    @Override
    public ClusterResourceConnector clusterResourceConnector() {
        return ambariClusterResourceConnector;
    }

    @Override
    public ClusterType clusterType() {
        return AmbariConstants.AMBARI_TYPE;
    }
}
