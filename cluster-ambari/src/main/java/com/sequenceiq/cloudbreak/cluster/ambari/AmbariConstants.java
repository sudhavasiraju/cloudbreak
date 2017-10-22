package com.sequenceiq.cloudbreak.cluster.ambari;

import com.sequenceiq.cloudbreak.cluster.model.ClusterType;

public class AmbariConstants {
    public static final String AMBARI = "AMBARI";

    public static final ClusterType AMBARI_TYPE = ClusterType.clusterType(AMBARI);

    private AmbariConstants() {
    }
}
