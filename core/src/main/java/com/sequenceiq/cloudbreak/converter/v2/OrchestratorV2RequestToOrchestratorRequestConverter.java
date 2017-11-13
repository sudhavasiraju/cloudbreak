package com.sequenceiq.cloudbreak.converter.v2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.sequenceiq.cloudbreak.api.model.OrchestratorRequest;
import com.sequenceiq.cloudbreak.api.model.v2.OrchestratorV2Request;
import com.sequenceiq.cloudbreak.converter.AbstractConversionServiceAwareConverter;

@Component
public class OrchestratorV2RequestToOrchestratorRequestConverter extends AbstractConversionServiceAwareConverter<OrchestratorV2Request, OrchestratorRequest> {

    private static final Logger LOGGER = LoggerFactory.getLogger(OrchestratorV2RequestToOrchestratorRequestConverter.class);

    @Override
    public OrchestratorRequest convert(OrchestratorV2Request source) {
        OrchestratorRequest orchestratorRequest = new OrchestratorRequest();
        orchestratorRequest.setApiEndpoint(source.getApiEndpoint());
        orchestratorRequest.setParameters(source.getParameters());
        orchestratorRequest.setType("SALT");
        return orchestratorRequest;
    }
}
