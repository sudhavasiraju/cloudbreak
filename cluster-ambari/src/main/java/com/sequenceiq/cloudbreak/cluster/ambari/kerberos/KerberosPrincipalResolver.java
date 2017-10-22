package com.sequenceiq.cloudbreak.cluster.ambari.kerberos;

import org.springframework.stereotype.Service;

import com.google.common.base.Strings;
import com.sequenceiq.cloudbreak.domain.KerberosConfig;

@Service
public class KerberosPrincipalResolver {

    private static final String PRINCIPAL = "/admin";

    public String resolvePrincipalForKerberos(KerberosConfig kerberosConfig) {
        return Strings.isNullOrEmpty(kerberosConfig.getKerberosPrincipal()) ? kerberosConfig.getKerberosAdmin() + PRINCIPAL
                : kerberosConfig.getKerberosPrincipal();
    }
}
