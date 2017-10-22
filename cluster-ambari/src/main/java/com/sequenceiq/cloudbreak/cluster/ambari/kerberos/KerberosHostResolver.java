package com.sequenceiq.cloudbreak.cluster.ambari.kerberos;

import org.springframework.stereotype.Service;

import com.google.common.base.Strings;
import com.sequenceiq.cloudbreak.domain.Cluster;

@Service
public class KerberosHostResolver {

    public String resolveHostForKerberos(Cluster cluster, String gatewayHost) {
        return Strings.isNullOrEmpty(cluster.getKerberosConfig().getKerberosUrl()) ? gatewayHost : cluster.getKerberosConfig().getKerberosUrl();
    }
}
