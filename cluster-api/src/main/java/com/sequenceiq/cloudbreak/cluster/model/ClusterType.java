package com.sequenceiq.cloudbreak.cluster.model;

import com.sequenceiq.cloudbreak.cluster.model.generic.StringType;

public class ClusterType extends StringType {

    private ClusterType(String clusterType) {
        super(clusterType);
    }

    public static ClusterType clusterType(String clusterType) {
        return new ClusterType(clusterType);
    }
}
