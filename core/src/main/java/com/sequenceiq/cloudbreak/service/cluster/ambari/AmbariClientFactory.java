package com.sequenceiq.cloudbreak.service.cluster.ambari;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.sequenceiq.ambari.client.AmbariClient;
import com.sequenceiq.cloudbreak.client.HttpClientConfig;
import com.sequenceiq.cloudbreak.core.CloudbreakSecuritySetupException;
import com.sequenceiq.cloudbreak.domain.Cluster;
import com.sequenceiq.cloudbreak.domain.Stack;
import com.sequenceiq.cloudbreak.service.TlsSecurityService;
import com.sequenceiq.cloudbreak.service.cluster.AmbariClientProvider;

@Component
public class AmbariClientFactory {

    @Inject
    private AmbariClientProvider ambariClientProvider;

    @Inject
    private TlsSecurityService tlsSecurityService;

    public AmbariClient getDefaultAmbariClient(Stack stack)
            throws CloudbreakSecuritySetupException {
        HttpClientConfig clientConfig = tlsSecurityService.buildTLSClientConfigForPrimaryGateway(stack.getId(), stack.getAmbariIp());
        return ambariClientProvider.getDefaultAmbariClient(clientConfig, stack.getGatewayPort());
    }

    public AmbariClient getAmbariClient(Stack stack, Cluster cluster)
            throws CloudbreakSecuritySetupException {
        HttpClientConfig clientConfig = tlsSecurityService.buildTLSClientConfigForPrimaryGateway(stack.getId(), stack.getAmbariIp());
        return ambariClientProvider.getAmbariClient(clientConfig, stack.getGatewayPort(), cluster);
    }

    public AmbariClient getAmbariClient(Stack stack, String username, String password)
            throws CloudbreakSecuritySetupException {
        HttpClientConfig clientConfig = tlsSecurityService.buildTLSClientConfigForPrimaryGateway(stack.getId(), stack.getAmbariIp());
        return ambariClientProvider.getAmbariClient(clientConfig, stack.getGatewayPort(), username, password);
    }
}
