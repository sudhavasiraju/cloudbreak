package com.sequenceiq.cloudbreak.cluster.ambari.blueprint.provider;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.sequenceiq.cloudbreak.cluster.model.BlueprintConfigurationEntry;
import com.sequenceiq.cloudbreak.cluster.ambari.blueprint.BlueprintProcessor;
import com.sequenceiq.cloudbreak.domain.Stack;

@Component
public class LlapConfigProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(LlapConfigProvider.class);

    @Inject
    private BlueprintProcessor blueprintProcessor;

    public String addToBlueprint(Stack stack, String blueprintText) {
        if (blueprintProcessor.componentExistsInBlueprint("HIVE_SERVER_INTERACTIVE", blueprintText)) {
            LOGGER.info("Hive server interactive exists in Blueprint");
            List<BlueprintConfigurationEntry> configs = getConfigs(stack.getFullNodeCount() - 1);
            blueprintText = blueprintProcessor.addConfigEntries(blueprintText, configs, false);
        }
        return blueprintText;
    }

    private List<BlueprintConfigurationEntry> getConfigs(Integer nodeCount) {
        List<BlueprintConfigurationEntry> configs = new ArrayList<>();
        configs.add(new BlueprintConfigurationEntry("hive-interactive-env", "num_llap_nodes", nodeCount.toString()));
        return configs;
    }
}
