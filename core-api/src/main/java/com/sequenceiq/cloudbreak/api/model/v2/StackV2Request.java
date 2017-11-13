package com.sequenceiq.cloudbreak.api.model.v2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import javax.validation.constraints.Size;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.sequenceiq.cloudbreak.api.model.JsonEntity;
import com.sequenceiq.cloudbreak.api.model.StackAuthenticationRequest;
import com.sequenceiq.cloudbreak.doc.ModelDescriptions.StackModelDescription;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

@ApiModel
@JsonIgnoreProperties(ignoreUnknown = true)
public class StackV2Request implements JsonEntity {
    @Size(max = 40, min = 5, message = "The length of the name has to be in range of 5 to 40")
    @Pattern(regexp = "([a-z][-a-z0-9]*[a-z0-9])",
            message = "The name can only contain lowercase alphanumeric characters and hyphens and has start with an alphanumeric character")
    @NotNull
    @ApiModelProperty(value = StackModelDescription.STACK_NAME, required = true, position = 0)
    private String name;

    @ApiModelProperty(value = StackModelDescription.CREDENTIAL_NAME, position = 1)
    private String credentialName;

    @ApiModelProperty(value = StackModelDescription.REGION, position = 2)
    private String region;

    @ApiModelProperty(value = StackModelDescription.AVAILABILITY_ZONE, position = 3)
    private String availabilityZone;

    @ApiModelProperty(value = StackModelDescription.AUTHENTICATION, position = 4)
    private StackAuthenticationRequest stackAuthentication;

    @ApiModelProperty(value = StackModelDescription.PLATFORM_VARIANT, position = 5)
    private String platformVariant;

    @ApiModelProperty(value = StackModelDescription.IMAGE_CATALOG, position = 6)
    private String imageCatalog;

    @ApiModelProperty(value = StackModelDescription.IMAGE_ID, position = 7)
    private String imageId;

    @ApiModelProperty(value = StackModelDescription.NETWORK, position = 8)
    private NetworkV2Request network;

    @Valid
    @ApiModelProperty(value = StackModelDescription.ORCHESTRATOR, position = 9)
    private OrchestratorV2Request orchestrator;

    @Valid
    @ApiModelProperty(value = StackModelDescription.INSTANCE_GROUPS, required = true, position = 10)
    private List<InstanceGroupV2Request> instanceGroups = new ArrayList<>();

    @Valid
    @ApiModelProperty(value = StackModelDescription.CLUSTER_REQUEST, position = 11)
    private ClusterV2Request clusterRequest;

    @ApiModelProperty(value = StackModelDescription.PARAMETERS, position = 12)
    private Map<String, String> parameters = new HashMap<>();

    @ApiModelProperty(value = StackModelDescription.APPLICATION_TAGS, position = 13)
    private Map<String, String> applicationTags = new HashMap<>();

    @ApiModelProperty(value = StackModelDescription.USERDEFINED_TAGS, position = 14)
    private Map<String, String> userDefinedTags = new HashMap<>();

    @ApiModelProperty(value = StackModelDescription.DEFAULT_TAGS, position = 15)
    private Map<String, String> defaultTags = new HashMap<>();

    @ApiModelProperty(value = StackModelDescription.FLEX_ID, position = 16)
    private Long flexId;

    @ApiModelProperty(value = StackModelDescription.AMBARI_VERSION, position = 17)
    private String ambariVersion;

    @ApiModelProperty(value = StackModelDescription.HDP_VERSION, position = 18)
    private String hdpVersion;

    @ApiModelProperty(value = StackModelDescription.CUSTOM_DOMAIN, position = 19)
    private String customDomain;

    @ApiModelProperty(value = StackModelDescription.CUSTOM_HOSTNAME, position = 20)
    private String customHostname;

    @ApiModelProperty(value = StackModelDescription.CLUSTER_NAME_AS_SUBDOMAIN, position = 21)
    private boolean clusterNameAsSubdomain;

    @ApiModelProperty(value = StackModelDescription.HOSTGROUP_NAME_AS_HOSTNAME, position = 22)
    private boolean hostgroupNameAsHostname;

    @ApiModelProperty(hidden = true)
    private String owner;

    @ApiModelProperty(hidden = true)
    private String account;

    public OrchestratorV2Request getOrchestrator() {
        return orchestrator;
    }

    public void setOrchestrator(OrchestratorV2Request orchestrator) {
        this.orchestrator = orchestrator;
    }

    public String getImageCatalog() {
        return imageCatalog;
    }

    public void setImageCatalog(String imageCatalog) {
        this.imageCatalog = imageCatalog;
    }

    public List<InstanceGroupV2Request> getInstanceGroups() {
        return instanceGroups;
    }

    public void setInstanceGroups(List<InstanceGroupV2Request> instanceGroups) {
        this.instanceGroups = instanceGroups;
    }

    public String getImageId() {
        return imageId;
    }

    public void setImageId(String imageId) {
        this.imageId = imageId;
    }

    public Long getFlexId() {
        return flexId;
    }

    public void setFlexId(Long flexId) {
        this.flexId = flexId;
    }

    public ClusterV2Request getClusterRequest() {
        return clusterRequest;
    }

    public void setClusterRequest(ClusterV2Request clusterRequest) {
        this.clusterRequest = clusterRequest;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getAccount() {
        return account;
    }

    public void setAccount(String account) {
        this.account = account;
    }

    public StackAuthenticationRequest getStackAuthentication() {
        return stackAuthentication;
    }

    public void setStackAuthentication(StackAuthenticationRequest stackAuthentication) {
        this.stackAuthentication = stackAuthentication;
    }

    public String getCredentialName() {
        return credentialName;
    }

    public void setCredentialName(String credentialName) {
        this.credentialName = credentialName;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public String getAvailabilityZone() {
        return availabilityZone;
    }

    public void setAvailabilityZone(String availabilityZone) {
        this.availabilityZone = availabilityZone;
    }

    public Map<String, String> getParameters() {
        return parameters;
    }

    public void setParameters(Map<String, String> parameters) {
        this.parameters = parameters;
    }

    public String getPlatformVariant() {
        return platformVariant;
    }

    public void setPlatformVariant(String platformVariant) {
        this.platformVariant = platformVariant;
    }

    public String getAmbariVersion() {
        return ambariVersion;
    }

    public void setAmbariVersion(String ambariVersion) {
        this.ambariVersion = ambariVersion;
    }

    public String getHdpVersion() {
        return hdpVersion;
    }

    public void setHdpVersion(String hdpVersion) {
        this.hdpVersion = hdpVersion;
    }

    public String getCustomDomain() {
        return customDomain;
    }

    public void setCustomDomain(String customDomain) {
        this.customDomain = customDomain;
    }

    public String getCustomHostname() {
        return customHostname;
    }

    public void setCustomHostname(String customHostname) {
        this.customHostname = customHostname;
    }

    public boolean isClusterNameAsSubdomain() {
        return clusterNameAsSubdomain;
    }

    public void setClusterNameAsSubdomain(boolean clusterNameAsSubdomain) {
        this.clusterNameAsSubdomain = clusterNameAsSubdomain;
    }

    public boolean isHostgroupNameAsHostname() {
        return hostgroupNameAsHostname;
    }

    public void setHostgroupNameAsHostname(boolean hostgroupNameAsHostname) {
        this.hostgroupNameAsHostname = hostgroupNameAsHostname;
    }

    public Map<String, String> getApplicationTags() {
        return applicationTags;
    }

    public void setApplicationTags(Map<String, String> applicationTags) {
        this.applicationTags = applicationTags;
    }

    public Map<String, String> getUserDefinedTags() {
        return userDefinedTags;
    }

    public void setUserDefinedTags(Map<String, String> userDefinedTags) {
        this.userDefinedTags = userDefinedTags;
    }

    public Map<String, String> getDefaultTags() {
        return defaultTags;
    }

    public void setDefaultTags(Map<String, String> defaultTags) {
        this.defaultTags = defaultTags;
    }

    public NetworkV2Request getNetwork() {
        return network;
    }

    public void setNetwork(NetworkV2Request network) {
        this.network = network;
    }
}
