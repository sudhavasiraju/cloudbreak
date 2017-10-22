package com.sequenceiq.cloudbreak.cluster.ambari.kerberos;

import org.springframework.stereotype.Service;

import com.google.common.base.Strings;
import com.sequenceiq.cloudbreak.domain.KerberosConfig;

@Service
public class KerberosContainerDnResolver {

    public String resolveContainerDnForKerberos(KerberosConfig kerberosConfig) {
        return Strings.isNullOrEmpty(kerberosConfig.getKerberosContainerDn()) ? null : kerberosConfig.getKerberosContainerDn();
    }
}
