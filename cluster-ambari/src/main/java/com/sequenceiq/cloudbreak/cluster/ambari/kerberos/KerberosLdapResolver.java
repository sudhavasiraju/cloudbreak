package com.sequenceiq.cloudbreak.cluster.ambari.kerberos;

import org.springframework.stereotype.Service;

import com.google.common.base.Strings;
import com.sequenceiq.cloudbreak.domain.KerberosConfig;

@Service
public class KerberosLdapResolver {

    public String resolveLdapUrlForKerberos(KerberosConfig kerberosConfig) {
        return Strings.isNullOrEmpty(kerberosConfig.getKerberosLdapUrl()) ? null : kerberosConfig.getKerberosLdapUrl();
    }
}
