package com.sequenceiq.cloudbreak.core.cluster;

import java.util.Optional;

import javax.inject.Inject;

import org.springframework.stereotype.Service;

import com.sequenceiq.cloudbreak.CloudbreakException;
import com.sequenceiq.cloudbreak.cluster.ambari.task.AmbariClusterConnector;
import com.sequenceiq.cloudbreak.common.model.OrchestratorType;
import com.sequenceiq.cloudbreak.core.bootstrap.service.OrchestratorTypeResolver;
import com.sequenceiq.cloudbreak.core.bootstrap.service.container.ContainerOrchestratorResolver;
import com.sequenceiq.cloudbreak.core.bootstrap.service.host.HostOrchestratorResolver;
import com.sequenceiq.cloudbreak.domain.Stack;
import com.sequenceiq.cloudbreak.service.smartsense.SmartSenseSubscriptionService;
import com.sequenceiq.cloudbreak.service.stack.StackService;

@Service
public class AmbariClusterCreationService {
    @Inject
    private StackService stackService;

    @Inject
    private AmbariClusterConnector ambariClusterConnector;

    @Inject
    private SmartSenseSubscriptionService smartSenseSubscriptionService;

    @Inject
    private HostOrchestratorResolver hostOrchestratorResolver;

    @Inject
    private ContainerOrchestratorResolver containerOrchestratorResolver;

    @Inject
    private OrchestratorTypeResolver orchestratorTypeResolver;

    public void startAmbari(Long stackId) throws CloudbreakException {
        Stack stack = stackService.getById(stackId);
        ambariClusterConnector.waitForAmbariServer(stack);
        ambariClusterConnector.changeOriginalAmbariCredentialsAndCreateCloudbreakUser(stack);
    }

    public void buildAmbariCluster(Long stackId) throws CloudbreakException {
        Stack stack = stackService.getByIdWithLists(stackId);
        OrchestratorType orchestratorType = orchestratorTypeResolver.resolveType(stack.getOrchestrator().getType());
        if (orchestratorType.hostOrchestrator()) {
            ambariClusterConnector.buildAmbariCluster(stack, Optional.of(hostOrchestratorResolver.get(stack.getOrchestrator().getType())), orchestratorType);
        } else {
            ambariClusterConnector.buildAmbariCluster(stack, Optional.empty(), orchestratorType);
        }

    }
}
