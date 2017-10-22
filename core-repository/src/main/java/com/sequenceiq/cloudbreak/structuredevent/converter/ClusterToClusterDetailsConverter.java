package com.sequenceiq.cloudbreak.structuredevent.converter;

import java.io.IOException;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.springframework.core.convert.ConversionService;
import org.springframework.stereotype.Component;

import com.sequenceiq.cloudbreak.api.model.DatabaseVendor;
import com.sequenceiq.cloudbreak.cloud.model.AmbariDatabase;
import com.sequenceiq.cloudbreak.cloud.model.AmbariRepo;
import com.sequenceiq.cloudbreak.cloud.model.HDPRepo;
import com.sequenceiq.cloudbreak.common.type.ComponentType;
import com.sequenceiq.cloudbreak.converter.AbstractConversionServiceAwareConverter;
import com.sequenceiq.cloudbreak.domain.Cluster;
import com.sequenceiq.cloudbreak.domain.ClusterComponent;
import com.sequenceiq.cloudbreak.domain.FileSystem;
import com.sequenceiq.cloudbreak.domain.Gateway;
import com.sequenceiq.cloudbreak.domain.KerberosConfig;
import com.sequenceiq.cloudbreak.repository.ClusterComponentRepository;
import com.sequenceiq.cloudbreak.structuredevent.event.ClusterDetails;
import com.sequenceiq.cloudbreak.task.CloudbreakServiceException;

@Component
public class ClusterToClusterDetailsConverter extends AbstractConversionServiceAwareConverter<Cluster, ClusterDetails> {
    @Inject
    private ConversionService conversionService;

    @Inject
    private ClusterComponentRepository componentRepository;

    @Override
    public ClusterDetails convert(Cluster source) {
        ClusterDetails clusterDetails = new ClusterDetails();
        clusterDetails.setId(source.getId());
        clusterDetails.setName(source.getName());
        clusterDetails.setDescription(source.getDescription());
        clusterDetails.setStatus(source.getStatus().toString());
        clusterDetails.setStatusReason(source.getStatusReason());
        convertKerberosConfig(clusterDetails, source);
        convertGatewayProperties(clusterDetails, source.getGateway());
        convertFileSystemProperties(clusterDetails, source.getFileSystem());
        convertComponents(clusterDetails, source);
        return clusterDetails;
    }

    private void convertKerberosConfig(ClusterDetails clusterDetails, Cluster source) {
        Boolean secure = source.isSecure();
        clusterDetails.setSecure(secure);
        if (secure) {
            clusterDetails.setSecure(Boolean.TRUE);
            KerberosConfig kerberosConfig = source.getKerberosConfig();
            String kerberosType = "New MIT Kerberos";
            if (kerberosConfig != null) {
                if (StringUtils.isNoneEmpty(kerberosConfig.getKerberosUrl())) {
                    if (StringUtils.isNoneEmpty(kerberosConfig.getKerberosLdapUrl())) {
                        kerberosType = "Existing Active Directory";
                    } else {
                        kerberosType = "Existing MIT Kerberos";
                    }
                }
            }
            clusterDetails.setKerberosType(kerberosType);
        }
    }

    private void convertGatewayProperties(ClusterDetails clusterDetails, Gateway gateway) {
        if (gateway != null) {
            clusterDetails.setGatewayEnabled(gateway.getEnableGateway());
            clusterDetails.setGatewayType(gateway.getGatewayType().toString());
            clusterDetails.setSsoType(gateway.getSsoType().toString());
        } else {
            clusterDetails.setGatewayEnabled(false);
        }
    }

    private void convertFileSystemProperties(ClusterDetails clusterDetails, FileSystem fileSystem) {
        if (fileSystem != null) {
            clusterDetails.setFileSystemType(fileSystem.getType());
            clusterDetails.setDefaultFileSystem(fileSystem.isDefaultFs());
        }
    }

    private void convertComponents(ClusterDetails clusterDetails, Cluster cluster) {
        AmbariRepo ambariRepo = getComponent(cluster.getId(), ComponentType.AMBARI_REPO_DETAILS, AmbariRepo.class);
        if (ambariRepo != null) {
            clusterDetails.setAmbariVersion(ambariRepo.getVersion());
        }
        HDPRepo hdpRepo = getComponent(cluster.getId(), ComponentType.HDP_REPO_DETAILS, HDPRepo.class);
        if (hdpRepo != null) {
            clusterDetails.setClusterType(hdpRepo.getStack().get(HDPRepo.REPO_ID_TAG));
            clusterDetails.setClusterVersion(hdpRepo.getHdpVersion());
        }
        AmbariDatabase ambariDatabase = getComponent(cluster.getId(), ComponentType.AMBARI_DATABASE_DETAILS, AmbariDatabase.class);
        if (ambariDatabase != null) {
            clusterDetails.setExternalDatabase(!ambariDatabase.getVendor().equals(DatabaseVendor.EMBEDDED.value()));
            clusterDetails.setDatabaseType(ambariDatabase.getVendor());
        } else {
            clusterDetails.setExternalDatabase(Boolean.FALSE);
            clusterDetails.setDatabaseType(DatabaseVendor.EMBEDDED.value());
        }
    }

    public <T> T getComponent(Long clusterId, ComponentType componentType, Class<T> t) {
        try {
            ClusterComponent component = componentRepository.findComponentByClusterIdComponentTypeName(clusterId, componentType, componentType.name());
            if (component == null) {
                return null;
            }
            return component.getAttributes().get(t);
        } catch (IOException e) {
            throw new CloudbreakServiceException("Failed to read " + t.getClass() + " repo details for stack.", e);
        }
    }
}
