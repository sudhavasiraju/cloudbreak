package com.sequenceiq.cloudbreak.cluster.ambari;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.sequenceiq.cloudbreak.cloud.model.AmbariDatabase;
import com.sequenceiq.cloudbreak.cloud.model.AmbariRepo;
import com.sequenceiq.cloudbreak.cloud.model.HDPRepo;
import com.sequenceiq.cloudbreak.common.type.ComponentType;
import com.sequenceiq.cloudbreak.domain.Cluster;
import com.sequenceiq.cloudbreak.domain.ClusterComponent;
import com.sequenceiq.cloudbreak.repository.ClusterComponentRepository;
import com.sequenceiq.cloudbreak.task.CloudbreakServiceException;

@Service
public class AmbariComponentConfigProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(AmbariComponentConfigProvider.class);

    @Inject
    private ClusterComponentRepository componentRepository;

    public ClusterComponent getComponent(Long clusterId, ComponentType componentType) {
        return getComponent(clusterId, componentType, componentType.name());
    }

    public ClusterComponent getComponent(Long clusterId, ComponentType componentType, String name) {
        return componentRepository.findComponentByClusterIdComponentTypeName(clusterId, componentType, name);
    }

    public AmbariRepo getAmbariRepo(Long clusterId) {
        try {
            ClusterComponent component = getComponent(clusterId, ComponentType.AMBARI_REPO_DETAILS);
            if (component == null) {
                return null;
            }
            return component.getAttributes().get(AmbariRepo.class);
        } catch (IOException e) {
            throw new CloudbreakServiceException("Failed to read ambari repo details for stack.", e);
        }
    }

    public HDPRepo getHDPRepo(Long clusterId) {
        try {
            ClusterComponent component = getComponent(clusterId, ComponentType.HDP_REPO_DETAILS);
            if (component == null) {
                return null;
            }
            return component.getAttributes().get(HDPRepo.class);
        } catch (IOException e) {
            throw new CloudbreakServiceException("Failed to read hdp repo details for stack.", e);
        }
    }

    public AmbariDatabase getAmbariDatabase(Long clusterId) {
        try {
            ClusterComponent component = getComponent(clusterId, ComponentType.AMBARI_DATABASE_DETAILS);
            if (component == null) {
                return null;
            }
            return component.getAttributes().get(AmbariDatabase.class);
        } catch (IOException e) {
            throw new CloudbreakServiceException("Failed to read Ambari database", e);
        }
    }

    public <T> T getComponent(List<ClusterComponent> components, Class<T> clazz, ComponentType componentType) {
        try {
            Optional<ClusterComponent> comp = components.stream().filter(
                    c -> c.getComponentType() == componentType).findFirst();
            return comp.isPresent() ? comp.get().getAttributes().get(clazz) : null;
        } catch (IOException e) {
            throw new CloudbreakServiceException("Failed to read component", e);
        }
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
