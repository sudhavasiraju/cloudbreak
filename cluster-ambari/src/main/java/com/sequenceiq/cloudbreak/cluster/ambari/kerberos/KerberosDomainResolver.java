package com.sequenceiq.cloudbreak.cluster.ambari.kerberos;

import org.springframework.stereotype.Service;

@Service
public class KerberosDomainResolver {

    public String getDomains(String gwDomain) {
        return '.' + gwDomain;
    }
}
