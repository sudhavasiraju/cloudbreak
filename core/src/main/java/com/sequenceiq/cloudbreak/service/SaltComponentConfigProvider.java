package com.sequenceiq.cloudbreak.service;

import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.sequenceiq.cloudbreak.common.type.ComponentType;
import com.sequenceiq.cloudbreak.domain.Cluster;
import com.sequenceiq.cloudbreak.domain.ClusterComponent;
import com.sequenceiq.cloudbreak.repository.ClusterComponentRepository;

@Service
public class SaltComponentConfigProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(SaltComponentConfigProvider.class);

    @Inject
    private ClusterComponentRepository componentRepository;

    public ClusterComponent getComponent(Long clusterId, ComponentType componentType) {
        return getComponent(clusterId, componentType, componentType.name());
    }

    public ClusterComponent getComponent(Long clusterId, ComponentType componentType, String name) {
        return componentRepository.findComponentByClusterIdComponentTypeName(clusterId, componentType, name);
    }

    public ClusterComponent store(ClusterComponent component) {
        LOGGER.debug("Component is going to be saved: {}", component);
        ClusterComponent ret = componentRepository.save(component);
        LOGGER.debug("Component saved: stackId: {}, component: {}", ret.getCluster().getId(), ret);
        return ret;
    }

    public List<ClusterComponent> store(List<ClusterComponent> components, Cluster cluster) {
        for (ClusterComponent component : components) {
            component.setCluster(cluster);
            store(component);
        }
        return components;
    }
}
