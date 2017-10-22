package com.sequenceiq.cloudbreak.reactor;

import java.util.Optional;
import java.util.Set;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.sequenceiq.cloudbreak.cluster.ambari.task.AmbariDecommissioner;
import com.sequenceiq.cloudbreak.core.bootstrap.service.OrchestratorTypeResolver;
import com.sequenceiq.cloudbreak.core.bootstrap.service.container.ContainerOrchestratorResolver;
import com.sequenceiq.cloudbreak.core.bootstrap.service.host.HostOrchestratorResolver;
import com.sequenceiq.cloudbreak.domain.Stack;
import com.sequenceiq.cloudbreak.orchestrator.container.ContainerOrchestrator;
import com.sequenceiq.cloudbreak.orchestrator.host.HostOrchestrator;
import com.sequenceiq.cloudbreak.reactor.api.event.EventSelectorUtil;
import com.sequenceiq.cloudbreak.reactor.api.event.resource.DecommissionRequest;
import com.sequenceiq.cloudbreak.reactor.api.event.resource.DecommissionResult;
import com.sequenceiq.cloudbreak.cloud.ReactorEventHandler;
import com.sequenceiq.cloudbreak.service.stack.StackService;

import reactor.bus.Event;
import reactor.bus.EventBus;

@Component
public class DecommissionHandler implements ReactorEventHandler<DecommissionRequest> {

    @Inject
    private EventBus eventBus;

    @Inject
    private StackService stackService;

    @Inject
    private AmbariDecommissioner ambariDecommissioner;

    @Inject
    private HostOrchestratorResolver hostOrchestratorResolver;

    @Inject
    private ContainerOrchestratorResolver containerOrchestratorResolver;

    @Inject
    private OrchestratorTypeResolver orchestratorTypeResolver;

    @Override
    public String selector() {
        return EventSelectorUtil.selector(DecommissionRequest.class);
    }

    @Override
    public void accept(Event<DecommissionRequest> event) {
        DecommissionRequest request = event.getData();
        DecommissionResult result;
        try {
            Stack stack = stackService.getByIdWithLists(request.getStackId());

            Optional<HostOrchestrator> hostOrchestrator = Optional.empty();
            Optional<ContainerOrchestrator> containerOrchestrator = Optional.empty();

            if (orchestratorTypeResolver.resolveType(stack.getOrchestrator()).containerOrchestrator()) {
                containerOrchestrator.orElse(containerOrchestratorResolver.get(stack.getOrchestrator().getType()));
            } else {
                hostOrchestrator.orElse(hostOrchestratorResolver.get(stack.getOrchestrator().getType()));
            }

            Set<String> hostNames = ambariDecommissioner.decommissionAmbariNodes(stack, request.getHostGroupName(), request.getHostNames(),
                    hostOrchestrator, containerOrchestrator);
            result = new DecommissionResult(request, hostNames);
        } catch (Exception e) {
            result = new DecommissionResult(e.getMessage(), e, request);
        }
        eventBus.notify(result.selector(), new Event<>(event.getHeaders(), result));
    }
}
