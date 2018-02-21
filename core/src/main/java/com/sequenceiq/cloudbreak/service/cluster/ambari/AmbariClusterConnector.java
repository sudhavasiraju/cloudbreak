package com.sequenceiq.cloudbreak.service.cluster.ambari;

import static com.sequenceiq.cloudbreak.service.PollingResult.isExited;
import static com.sequenceiq.cloudbreak.service.PollingResult.isSuccess;
import static com.sequenceiq.cloudbreak.service.PollingResult.isTimeout;
import static com.sequenceiq.cloudbreak.service.cluster.ambari.AmbariMessages.*;
import static com.sequenceiq.cloudbreak.service.cluster.ambari.AmbariMessages.AMBARI_CLUSTER_DISABLE_KERBEROS_FAILED;
import static com.sequenceiq.cloudbreak.service.cluster.ambari.AmbariMessages.AMBARI_CLUSTER_HOST_JOIN_FAILED;
import static com.sequenceiq.cloudbreak.service.cluster.ambari.AmbariMessages.AMBARI_CLUSTER_INSTALL_FAILED;
import static com.sequenceiq.cloudbreak.service.cluster.ambari.AmbariMessages.AMBARI_CLUSTER_PREPARE_DEKERBERIZING_FAILED;
import static com.sequenceiq.cloudbreak.service.cluster.ambari.AmbariMessages.AMBARI_CLUSTER_SERVICES_STARTED;
import static com.sequenceiq.cloudbreak.service.cluster.ambari.AmbariMessages.AMBARI_CLUSTER_SERVICES_STARTING;
import static com.sequenceiq.cloudbreak.service.cluster.ambari.AmbariMessages.AMBARI_CLUSTER_UPSCALE_FAILED;
import static com.sequenceiq.cloudbreak.service.cluster.ambari.AmbariOperationType.DISABLE_KERBEROS_STATE;
import static com.sequenceiq.cloudbreak.service.cluster.ambari.AmbariOperationType.INSTALL_AMBARI_PROGRESS_STATE;
import static com.sequenceiq.cloudbreak.service.cluster.ambari.AmbariOperationType.PREPARE_DEKERBERIZING;
import static com.sequenceiq.cloudbreak.service.cluster.ambari.AmbariOperationType.START_AMBARI_PROGRESS_STATE;
import static com.sequenceiq.cloudbreak.service.cluster.ambari.AmbariOperationType.START_OPERATION_STATE;
import static com.sequenceiq.cloudbreak.service.cluster.ambari.AmbariOperationType.STOP_AMBARI_PROGRESS_STATE;
import static com.sequenceiq.cloudbreak.service.cluster.ambari.AmbariOperationType.UPSCALE_AMBARI_PROGRESS_STATE;
import static java.util.Collections.singletonMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.sequenceiq.ambari.client.AmbariClient;
import com.sequenceiq.ambari.client.AmbariConnectionException;
import com.sequenceiq.ambari.client.services.BlueprintService;
import com.sequenceiq.ambari.client.services.ServiceAndHostService;
import com.sequenceiq.cloudbreak.api.model.ExecutorType;
import com.sequenceiq.cloudbreak.api.model.FileSystemConfiguration;
import com.sequenceiq.cloudbreak.api.model.FileSystemType;
import com.sequenceiq.cloudbreak.api.model.InstanceStatus;
import com.sequenceiq.cloudbreak.api.model.Status;
import com.sequenceiq.cloudbreak.blueprint.BlueprintConfigurationEntry;
import com.sequenceiq.cloudbreak.blueprint.BlueprintProcessor;
import com.sequenceiq.cloudbreak.blueprint.druid.DruidSupersetConfigProvider;
import com.sequenceiq.cloudbreak.blueprint.filesystem.AzureFileSystemConfigProvider;
import com.sequenceiq.cloudbreak.blueprint.filesystem.FileSystemConfigurator;
import com.sequenceiq.cloudbreak.blueprint.hadoop.HadoopConfigurationService;
import com.sequenceiq.cloudbreak.blueprint.hbase.HbaseConfigProvider;
import com.sequenceiq.cloudbreak.blueprint.kerberos.KerberosBlueprintService;
import com.sequenceiq.cloudbreak.blueprint.llap.LlapConfigProvider;
import com.sequenceiq.cloudbreak.blueprint.rds.RDSConfigProvider;
import com.sequenceiq.cloudbreak.blueprint.recovery.AutoRecoveryConfigProvider;
import com.sequenceiq.cloudbreak.blueprint.smartsense.SmartSenseConfigProvider;
import com.sequenceiq.cloudbreak.blueprint.template.BlueprintTemplateProcessor;
import com.sequenceiq.cloudbreak.blueprint.yarn.ContainerExecutorConfigProvider;
import com.sequenceiq.cloudbreak.blueprint.zeppelin.ZeppelinConfigProvider;
import com.sequenceiq.cloudbreak.cloud.model.component.StackRepoDetails;
import com.sequenceiq.cloudbreak.cloud.scheduler.CancellationException;
import com.sequenceiq.cloudbreak.common.service.user.UserFilterField;
import com.sequenceiq.cloudbreak.common.type.CloudConstants;
import com.sequenceiq.cloudbreak.common.type.HostMetadataState;
import com.sequenceiq.cloudbreak.controller.BadRequestException;
import com.sequenceiq.cloudbreak.core.CloudbreakException;
import com.sequenceiq.cloudbreak.core.CloudbreakSecuritySetupException;
import com.sequenceiq.cloudbreak.core.bootstrap.service.OrchestratorTypeResolver;
import com.sequenceiq.cloudbreak.domain.Blueprint;
import com.sequenceiq.cloudbreak.domain.Cluster;
import com.sequenceiq.cloudbreak.domain.FileSystem;
import com.sequenceiq.cloudbreak.domain.HostGroup;
import com.sequenceiq.cloudbreak.domain.HostMetadata;
import com.sequenceiq.cloudbreak.domain.InstanceGroup;
import com.sequenceiq.cloudbreak.domain.InstanceMetaData;
import com.sequenceiq.cloudbreak.domain.RDSConfig;
import com.sequenceiq.cloudbreak.domain.Stack;
import com.sequenceiq.cloudbreak.repository.HostMetadataRepository;
import com.sequenceiq.cloudbreak.repository.InstanceMetaDataRepository;
import com.sequenceiq.cloudbreak.service.CloudbreakServiceException;
import com.sequenceiq.cloudbreak.service.ClusterComponentConfigProvider;
import com.sequenceiq.cloudbreak.service.PollingResult;
import com.sequenceiq.cloudbreak.service.TlsSecurityService;
import com.sequenceiq.cloudbreak.service.cluster.ClusterService;
import com.sequenceiq.cloudbreak.service.cluster.api.ClusterApi;
import com.sequenceiq.cloudbreak.service.cluster.flow.AmbariOperationService;
import com.sequenceiq.cloudbreak.service.cluster.flow.RecipeEngine;
import com.sequenceiq.cloudbreak.service.cluster.flow.blueprint.HiveConfigProvider;
import com.sequenceiq.cloudbreak.service.events.CloudbreakEventService;
import com.sequenceiq.cloudbreak.service.hostgroup.HostGroupService;
import com.sequenceiq.cloudbreak.service.image.ImageService;
import com.sequenceiq.cloudbreak.service.messages.CloudbreakMessagesService;
import com.sequenceiq.cloudbreak.service.smartsense.SmartSenseSubscriptionService;
import com.sequenceiq.cloudbreak.service.stack.StackService;
import com.sequenceiq.cloudbreak.service.user.UserDetailsService;
import com.sequenceiq.cloudbreak.util.AmbariClientExceptionUtil;
import com.sequenceiq.cloudbreak.util.JsonUtil;

import groovyx.net.http.HttpResponseException;

@Service
public class AmbariClusterConnector implements ClusterApi {

    private static final Logger LOGGER = LoggerFactory.getLogger(AmbariClusterConnector.class);


    private static final String ADMIN = "admin";

    @Inject
    private StackService stackService;

    @Inject
    private ClusterService clusterService;

    @Inject
    private AmbariClientFactory clientFactory;

    @Inject
    private AmbariUserHandler ambariUserHandler;

    @Inject
    private AmbariClusterConnectorPollingResultChecker ambariClusterConnectorPollingResultChecker;

    @Inject
    private InstanceMetaDataRepository instanceMetadataRepository;

    @Inject
    private HostGroupService hostGroupService;

    @Inject
    private AmbariOperationService ambariOperationService;

    @Inject
    private HadoopConfigurationService hadoopConfigurationService;

    @Inject
    private RecipeEngine recipeEngine;

    @Inject
    private HostMetadataRepository hostMetadataRepository;

    @Inject
    private CloudbreakMessagesService cloudbreakMessagesService;

    @Resource
    private Map<FileSystemType, FileSystemConfigurator<FileSystemConfiguration>> fileSystemConfigurators;

    @Inject
    private BlueprintProcessor blueprintProcessor;

    @Inject
    private TlsSecurityService tlsSecurityService;

    @Inject
    private SmartSenseConfigProvider smartSenseConfigProvider;

    @Inject
    private ZeppelinConfigProvider zeppelinConfigProvider;

    @Inject
    private DruidSupersetConfigProvider druidSupersetConfigProvider;

    @Inject
    private LlapConfigProvider llapConfigProvider;

    @Inject
    private RDSConfigProvider rdsConfigProvider;

    @Inject
    private ContainerExecutorConfigProvider containerExecutorConfigProvider;

    @Inject
    private AutoRecoveryConfigProvider autoRecoveryConfigProvider;

    @Inject
    private AzureFileSystemConfigProvider azureFileSystemConfigProvider;

    @Inject
    private ImageService imageService;

    @Inject
    private ClusterComponentConfigProvider clusterComponentConfigProvider;

    @Inject
    private AmbariViewProvider ambariViewProvider;

    @Inject
    private KerberosBlueprintService kerberosBlueprintService;

    @Inject
    private AmbariSecurityConfigProvider ambariSecurityConfigProvider;

    @Inject
    private OrchestratorTypeResolver orchestratorTypeResolver;

    @Inject
    private BlueprintTemplateProcessor blueprintTemplateProcessor;

    @Inject
    private AmbariClusterTemplateService ambariClusterTemplateService;

    @Inject
    private AmbariRepositoryVersionService ambariRepositoryVersionService;

    @Inject
    private HiveConfigProvider hiveConfigProvider;

    @Inject
    private SmartSenseSubscriptionService smartSenseSubscriptionService;

    @Inject
    private UserDetailsService userDetailsService;

    @Inject
    private CloudbreakEventService eventService;

    @Inject
    private HbaseConfigProvider hbaseConfigProvider;

    @Inject
    private HostGroupAssociationBuilder hostGroupAssociationBuilder;

    @Inject
    private AmbariPollingServiceProvider ambariPollingServiceProvider;

    @Override
    public void waitForServer(Stack stack) throws CloudbreakException {
        AmbariClient defaultAmbariClient = clientFactory.getDefaultAmbariClient(stack);
        AmbariClient cloudbreakAmbariClient = clientFactory.getAmbariClient(stack, stack.getCluster());

        PollingResult pollingResult = ambariPollingServiceProvider.ambariStartupPollerObjectPollingService(stack, defaultAmbariClient, cloudbreakAmbariClient);
        if (isSuccess(pollingResult)) {
            LOGGER.info("Ambari has successfully started! Polling result: {}", pollingResult);
        } else if (isExited(pollingResult)) {
            throw new CancellationException("Polling of Ambari server start has been cancelled.");
        } else {
            LOGGER.info("Could not start Ambari. polling result: {}", pollingResult);
            throw new CloudbreakException(String.format("Could not start Ambari. polling result: '%s'", pollingResult));
        }
    }

    @Override
    public void buildCluster(Stack stack) {
        Cluster cluster = stack.getCluster();
        try {
            if (cluster.getCreationStarted() == null) {
                cluster.setCreationStarted(new Date().getTime());
                cluster = clusterService.updateCluster(cluster);
            }
            AmbariClient ambariClient = clientFactory.getAmbariClient(stack, stack.getCluster());

            Set<HostGroup> hostGroups = hostGroupService.getByCluster(cluster.getId());
            Map<String, List<Map<String, String>>> hostGroupMappings = hostGroupAssociationBuilder.buildHostGroupAssociations(hostGroups);

            recipeEngine.executePostAmbariStartRecipes(stack, hostGroups);

            String blueprintText = generateBlueprintText(stack, cluster);


            ambariRepositoryVersionService.setBaseRepoURL(stack.getName(), cluster.getId(), stack.getOrchestrator(), ambariClient);
            addBlueprint(stack, ambariClient, blueprintText, hostGroups);

            Set<HostMetadata> hostsInCluster = hostMetadataRepository.findHostsInCluster(cluster.getId());
            PollingResult waitForHostsResult = ambariPollingServiceProvider.hostsPollingService(stack, ambariClient, hostsInCluster);
            ambariClusterConnectorPollingResultChecker.checkPollingResult(waitForHostsResult, cloudbreakMessagesService.getMessage(AMBARI_CLUSTER_HOST_JOIN_FAILED.code()));

            ambariClusterTemplateService.addClusterTemplate(cluster, hostGroupMappings, ambariClient);
            Pair<PollingResult, Exception> pollingResult = ambariOperationService.waitForOperationsToStart(stack, ambariClient,
                    singletonMap("INSTALL_START", 1), START_OPERATION_STATE);
            String message = pollingResult.getRight() == null
                    ? cloudbreakMessagesService.getMessage(AMBARI_CLUSTER_INSTALL_FAILED.code())
                    : pollingResult.getRight().getMessage();
            ambariClusterConnectorPollingResultChecker.checkPollingResult(pollingResult.getLeft(), message);

            Map<String, Integer> clusterInstallRequest = new HashMap<>(1);
            clusterInstallRequest.put("CLUSTER_INSTALL", 1);
            Pair<PollingResult, Exception> pollingResultExceptionPair = ambariOperationService
                    .waitForOperations(stack, ambariClient, clusterInstallRequest, INSTALL_AMBARI_PROGRESS_STATE);

            ambariClusterConnectorPollingResultChecker.checkPollingResult(pollingResultExceptionPair.getLeft(),
                    cloudbreakMessagesService.getMessage(AMBARI_CLUSTER_INSTALL_FAILED.code()));
            recipeEngine.executePostInstall(stack);

            triggerSmartSenseCapture(ambariClient, blueprintText);
            cluster = ambariViewProvider.provideViewInformation(ambariClient, cluster);
            handleClusterCreationSuccess(stack, cluster);
        } catch (CancellationException cancellationException) {
            throw cancellationException;
        } catch (HttpResponseException hre) {
            throw new AmbariOperationFailedException("Ambari could not create the cluster: " + AmbariClientExceptionUtil.getErrorMessage(hre), hre);
        } catch (Exception e) {
            LOGGER.error("Error while building the Ambari cluster. Message {}, throwable: {}", e.getMessage(), e);
            throw new AmbariOperationFailedException(e.getMessage(), e);
        }
    }

    @Override
    public void waitForHosts(Stack stack) throws CloudbreakSecuritySetupException {
        AmbariClient ambariClient = clientFactory.getAmbariClient(stack, stack.getCluster());
        Set<HostMetadata> hostMetadata = hostMetadataRepository.findHostsInCluster(stack.getCluster().getId());
        ambariPollingServiceProvider.hostsPollingService(stack, ambariClient, hostMetadata);
    }

    @Override
    public void upscaleCluster(Stack stack, HostGroup hostGroup, Collection<HostMetadata> hostMetadata) throws CloudbreakException {
        AmbariClient ambariClient = clientFactory.getAmbariClient(stack, stack.getCluster());
        List<String> upscaleHostNames = hostMetadata
                .stream()
                .map(HostMetadata::getHostName)
                .collect(Collectors.toList())
                .stream()
                .filter(hostName -> !ambariClient.getClusterHosts().contains(hostName))
                .collect(Collectors.toList());
        if (!upscaleHostNames.isEmpty()) {
            recipeEngine.executePostAmbariStartRecipes(stack, Sets.newHashSet(hostGroup));
            Pair<PollingResult, Exception> pollingResult = ambariOperationService.waitForOperations(stack, ambariClient,
                    installServices(upscaleHostNames, stack, ambariClient, hostGroup.getName()), UPSCALE_AMBARI_PROGRESS_STATE);
            String message = pollingResult.getRight() == null
                    ? cloudbreakMessagesService.getMessage(AMBARI_CLUSTER_UPSCALE_FAILED.code())
                    : pollingResult.getRight().getMessage();
            ambariClusterConnectorPollingResultChecker.checkPollingResult(pollingResult.getLeft(), message);
        }
    }

    @Override
    public void stopCluster(Stack stack) throws CloudbreakException {
        AmbariClient ambariClient = clientFactory.getAmbariClient(stack, stack.getCluster());
        try {
            boolean stopped = true;
            Collection<Map<String, String>> values = ambariClient.getHostComponentsStates().values();
            for (Map<String, String> value : values) {
                for (String state : value.values()) {
                    if (!"INSTALLED".equals(state)) {
                        stopped = false;
                    }
                }
            }
            if (!stopped) {
                LOGGER.info("Stop all Hadoop services");
                eventService.fireCloudbreakEvent(stack.getId(), Status.UPDATE_IN_PROGRESS.name(),
                        cloudbreakMessagesService.getMessage(AMBARI_CLUSTER_SERVICES_STOPPING.code()));
                int requestId = ambariClient.stopAllServices();
                if (requestId != -1) {
                    LOGGER.info("Waiting for Hadoop services to stop on stack");
                    PollingResult servicesStopResult = ambariOperationService.waitForOperations(stack, ambariClient, singletonMap("stop services", requestId),
                            STOP_AMBARI_PROGRESS_STATE).getLeft();
                    if (isExited(servicesStopResult)) {
                        throw new CancellationException("Cluster was terminated while waiting for Hadoop services to start");
                    } else if (isTimeout(servicesStopResult)) {
                        throw new CloudbreakException("Timeout while stopping Ambari services.");
                    }
                } else {
                    LOGGER.warn("Failed to stop Hadoop services.");
                    throw new CloudbreakException("Failed to stop Hadoop services.");
                }
                eventService.fireCloudbreakEvent(stack.getId(), Status.UPDATE_IN_PROGRESS.name(),
                        cloudbreakMessagesService.getMessage(AMBARI_CLUSTER_SERVICES_STOPPED.code()));
            }
        } catch (AmbariConnectionException ignored) {
            LOGGER.debug("Ambari not running on the gateway machine, no need to stop it.");
        }
    }

    @Override
    public int startCluster(Stack stack) throws CloudbreakException {
        AmbariClient ambariClient = clientFactory.getAmbariClient(stack, stack.getCluster());
        PollingResult ambariHealthCheckResult = ambariPollingServiceProvider.ambariHealthChecker(stack, ambariClient);
        if (isExited(ambariHealthCheckResult)) {
            throw new CancellationException("Cluster was terminated while waiting for Ambari to start.");
        } else if (isTimeout(ambariHealthCheckResult)) {
            throw new CloudbreakException("Ambari server was not restarted properly.");
        }
        LOGGER.info("Starting Ambari agents on the hosts.");
        Set<HostMetadata> hostsInCluster = hostMetadataRepository.findHostsInCluster(stack.getCluster().getId());
        PollingResult hostsJoinedResult = ambariPollingServiceProvider.ambariHostJoin(stack, ambariClient, hostsInCluster);
        if (PollingResult.EXIT.equals(hostsJoinedResult)) {
            throw new CancellationException("Cluster was terminated while starting Ambari agents.");
        }


        LOGGER.info("Start all Hadoop services");
        eventService.fireCloudbreakEvent(stack.getId(), Status.UPDATE_IN_PROGRESS.name(), cloudbreakMessagesService.getMessage(AMBARI_CLUSTER_SERVICES_STARTING.code()));
        int requestId = ambariClient.startAllServices();
        if (requestId == -1) {
            LOGGER.error("Failed to start Hadoop services.");
            throw new CloudbreakException("Failed to start Hadoop services.");
        }
        return requestId;
    }

    @Override
    public void prepareSecurity(Stack stack) {
        try {
            AmbariClient ambariClient = clientFactory.getAmbariClient(stack, stack.getCluster());
            Map<String, Integer> operationRequests = new HashMap<>();
            Stream.of("ZOOKEEPER", "HDFS", "YARN", "MAPREDUCE2", "KERBEROS").forEach(s -> {
                int opId = s.equals("ZOOKEEPER") ? ambariClient.startService(s) : ambariClient.stopService(s);
                if (opId != -1) {
                    operationRequests.put(s + "_SERVICE_STATE", opId);
                }
            });
            if (operationRequests.isEmpty()) {
                return;
            }
            ambariClusterConnectorPollingResultChecker.checkPollingResult(
                    ambariOperationService.waitForOperations(stack, ambariClient, operationRequests, PREPARE_DEKERBERIZING).getLeft(),
                    cloudbreakMessagesService.getMessage(AMBARI_CLUSTER_PREPARE_DEKERBERIZING_FAILED.code()));
        } catch (CancellationException cancellationException) {
            throw cancellationException;
        } catch (Exception e) {
            throw new AmbariOperationFailedException(e.getMessage(), e);
        }
    }

    @Override
    public void disableSecurity(Stack stack) {
        try {
            AmbariClient ambariClient = clientFactory.getAmbariClient(stack, stack.getCluster());
            int opId = ambariClient.disableKerberos();
            if (opId == -1) {
                return;
            }
            Map<String, Integer> operationRequests = singletonMap("DISABLE_KERBEROS_REQUEST", opId);
            PollingResult pollingResult = ambariOperationService.waitForOperations(stack, ambariClient, operationRequests, DISABLE_KERBEROS_STATE).getLeft();
            ambariClusterConnectorPollingResultChecker.checkPollingResult(pollingResult, cloudbreakMessagesService.getMessage(AMBARI_CLUSTER_DISABLE_KERBEROS_FAILED.code()));
        } catch (CancellationException cancellationException) {
            throw cancellationException;
        } catch (Exception e) {
            throw new AmbariOperationFailedException(e.getMessage(), e);
        }
    }

    @Override
    public void replaceUserNamePassword(Stack stack, String newUserName, String newPassword) throws CloudbreakException {
        AmbariClient ambariClient = clientFactory.getAmbariClient(stack, stack.getCluster().getUserName(), stack.getCluster().getPassword());
        ambariClient = ambariUserHandler.createAmbariUser(newUserName, newPassword, stack, ambariClient);
        ambariClient.deleteUser(stack.getCluster().getUserName());
    }

    @Override
    public void updateUserNamePassword(Stack stack, String newPassword) throws CloudbreakException {
        Cluster cluster = clusterService.getById(stack.getCluster().getId());
        AmbariClient ambariClient = clientFactory.getAmbariClient(stack, cluster.getUserName(), cluster.getPassword());
        changeAmbariPassword(cluster.getUserName(), cluster.getPassword(), newPassword, stack, ambariClient);
    }

    public void changeOriginalAmbariCredentialsAndCreateCloudbreakUser(Stack stack) throws CloudbreakException {
        Cluster cluster = stack.getCluster();
        LOGGER.info("Changing ambari credentials for cluster: {}, ambari ip: {}", cluster.getName(), cluster.getAmbariIp());
        String userName = cluster.getUserName();
        String password = cluster.getPassword();
        AmbariClient ambariClient = clientFactory.getDefaultAmbariClient(stack);
        String cloudbreakUserName = ambariSecurityConfigProvider.getAmbariUserName(cluster);
        String cloudbreakPassword = ambariSecurityConfigProvider.getAmbariPassword(cluster);
        ambariUserHandler.createAmbariUser(cloudbreakUserName, cloudbreakPassword, stack, ambariClient);
        if (ADMIN.equals(userName)) {
            if (!ADMIN.equals(password)) {
                changeAmbariPassword(ADMIN, ADMIN, password, stack, ambariClient);
            }
        } else {
            ambariClient = ambariUserHandler.createAmbariUser(userName, password, stack, ambariClient);
            ambariClient.deleteUser(ADMIN);
        }
    }

    @Override
    public void waitForServices(Stack stack, int requestId) throws CloudbreakException {
        AmbariClient ambariClient = clientFactory.getAmbariClient(stack, stack.getCluster());
        LOGGER.info("Waiting for Hadoop services to start on stack");
        PollingResult servicesStartResult = ambariOperationService.waitForOperations(stack, ambariClient, singletonMap("start services", requestId),
                START_AMBARI_PROGRESS_STATE).getLeft();
        if (isExited(servicesStartResult)) {
            throw new CancellationException("Cluster was terminated while waiting for Hadoop services to start");
        } else if (isTimeout(servicesStartResult)) {
            throw new CloudbreakException("Timeout while starting Ambari services.");
        }
        eventService.fireCloudbreakEvent(stack.getId(), Status.UPDATE_IN_PROGRESS.name(),
                cloudbreakMessagesService.getMessage(AMBARI_CLUSTER_SERVICES_STARTED.code()));
    }

    @Override
    public boolean available(Stack stack) throws CloudbreakSecuritySetupException {
        AmbariClient ambariClient = clientFactory.getAmbariClient(stack, stack.getCluster());
        return  ambariPollingServiceProvider.isAmbariAvailable(stack, ambariClient);
    }

    private String generateBlueprintText(Stack stack, Cluster cluster) throws IOException, CloudbreakException {
        Blueprint blueprint = cluster.getBlueprint();

        Set<RDSConfig> rdsConfigs = hiveConfigProvider.createPostgresRdsConfigIfNeeded(stack, cluster);

        String blueprintText = updateBlueprintWithInputs(cluster, blueprint, rdsConfigs);

        FileSystem fs = cluster.getFileSystem();
        blueprintText = updateBlueprintConfiguration(stack, blueprintText, rdsConfigs, fs);
        return blueprintText;
    }

    private String updateBlueprintConfiguration(Stack stack, String blueprintText, Set<RDSConfig> rdsConfigs, FileSystem fs)
            throws IOException, CloudbreakException {
        if (fs != null) {
            blueprintText = extendBlueprintWithFsConfig(blueprintText, fs, stack);
        }
        blueprintText = smartSenseConfigProvider.addToBlueprint(stack,
                blueprintText,
                smartSenseSubscriptionService.getDefault(),
                hostGroupService.getByCluster(stack.getCluster().getId()));
        blueprintText = zeppelinConfigProvider.addToBlueprint(stack,
                blueprintText,
                clusterComponentConfigProvider.getHDPRepo(stack.getCluster().getId()));
        blueprintText = druidSupersetConfigProvider.addToBlueprint(stack,
                blueprintText,
                userDetailsService.getDetails(stack.getCluster().getOwner(), UserFilterField.USERID).getUsername());
        // quick fix: this should be configured by StackAdvisor, but that's not working as of now
        blueprintText = llapConfigProvider.addToBlueprint(stack, blueprintText);
        if (!orchestratorTypeResolver.resolveType(stack.getOrchestrator()).containerOrchestrator()) {
            StackRepoDetails stackRepoDetails = clusterComponentConfigProvider.getHDPRepo(stack.getCluster().getId());
            if (stackRepoDetails != null && stackRepoDetails.getHdpVersion() != null) {
                blueprintText = blueprintProcessor.modifyHdpVersion(blueprintText, stackRepoDetails.getHdpVersion());
            }
        }
        if (rdsConfigs != null && !rdsConfigs.isEmpty()) {
            blueprintText = blueprintProcessor.addConfigEntries(blueprintText, rdsConfigProvider.getConfigs(rdsConfigs), true);
            blueprintText = blueprintProcessor.removeComponentFromBlueprint("MYSQL_SERVER", blueprintText);
        }
        if (ExecutorType.CONTAINER.equals(stack.getCluster().getExecutorType())) {
            blueprintText = containerExecutorConfigProvider.addToBlueprint(blueprintText);
        }
        blueprintText = autoRecoveryConfigProvider.addToBlueprint(blueprintText);
        return blueprintText;
    }

    private String updateBlueprintWithInputs(Cluster cluster, Blueprint blueprint, Set<RDSConfig> rdsConfigs) throws IOException {
        String blueprintText = blueprint.getBlueprintText();
        return blueprintTemplateProcessor
                .process(blueprintText, cluster, rdsConfigs, clusterComponentConfigProvider.getAmbariDatabase(cluster.getId()));
    }

    private void changeAmbariPassword(String userName, String oldPassword, String newPassword, Stack stack, AmbariClient ambariClient)
            throws CloudbreakException {
        try {
            ambariClient.changePassword(userName, oldPassword, newPassword, true);
        } catch (Exception e) {
            try {
                ambariClient = clientFactory.getAmbariClient(stack, userName, newPassword);
                ambariClient.ambariServerVersion();
            } catch (Exception ignored) {
                throw new CloudbreakException(e);
            }
        }
    }

    private String extendBlueprintWithFsConfig(String blueprintText, FileSystem fs, Stack stack) throws IOException {
        FileSystemType fileSystemType = FileSystemType.valueOf(fs.getType());
        FileSystemConfigurator<FileSystemConfiguration> fsConfigurator = fileSystemConfigurators.get(fileSystemType);
        String json = JsonUtil.writeValueAsString(fs.getProperties());
        FileSystemConfiguration fsConfiguration = JsonUtil.readValue(json, fileSystemType.getClazz());
        fsConfiguration = decorateFsConfigurationProperties(fsConfiguration, stack);
        Map<String, String> resourceProperties = fsConfigurator.createResources(fsConfiguration);
        List<BlueprintConfigurationEntry> bpConfigEntries = fsConfigurator.getFsProperties(fsConfiguration, resourceProperties);
        if (fs.isDefaultFs()) {
            bpConfigEntries.addAll(fsConfigurator.getDefaultFsProperties(fsConfiguration));
        }
        return blueprintProcessor.addConfigEntries(blueprintText, bpConfigEntries, true);
    }

    private FileSystemConfiguration decorateFsConfigurationProperties(FileSystemConfiguration fsConfiguration, Stack stack) {
        fsConfiguration.addProperty(FileSystemConfiguration.STORAGE_CONTAINER, "cloudbreak" + stack.getId());

        if (CloudConstants.AZURE.equals(stack.getPlatformVariant())) {
            fsConfiguration = azureFileSystemConfigProvider.decorateFileSystemConfiguration(stack, fsConfiguration);
        }
        return fsConfiguration;
    }

    private void handleClusterCreationSuccess(Stack stack, Cluster cluster) {
        LOGGER.info("Cluster created successfully. Cluster name: {}", cluster.getName());
        Long now = new Date().getTime();
        cluster.setCreationFinished(now);
        cluster.setUpSince(now);
        cluster = clusterService.updateCluster(cluster);
        Collection<InstanceMetaData> updatedInstances = new ArrayList<>();
        for (InstanceGroup instanceGroup : stack.getInstanceGroups()) {
            Set<InstanceMetaData> instances = instanceGroup.getAllInstanceMetaData();
            for (InstanceMetaData instanceMetaData : instances) {
                if (!instanceMetaData.isTerminated()) {
                    instanceMetaData.setInstanceStatus(InstanceStatus.REGISTERED);
                    updatedInstances.add(instanceMetaData);
                }
            }
        }
        instanceMetadataRepository.save(updatedInstances);
        Collection<HostMetadata> hostMetadata = new ArrayList<>();
        for (HostMetadata host : hostMetadataRepository.findHostsInCluster(cluster.getId())) {
            host.setHostMetadataState(HostMetadataState.HEALTHY);
            hostMetadata.add(host);
        }
        hostMetadataRepository.save(hostMetadata);
    }

    private void triggerSmartSenseCapture(ServiceAndHostService ambariClient, String blueprintText) {
        if (smartSenseConfigProvider.smartSenseIsConfigurable(blueprintText)) {
            try {
                LOGGER.info("Triggering SmartSense data capture.");
                ambariClient.smartSenseCapture(0);
            } catch (Exception e) {
                LOGGER.error("Triggering SmartSense capture is failed.", e);
            }
        }
    }

    private void addBlueprint(Stack stack, AmbariClient ambariClient, String blueprintText, Set<HostGroup> hostGroups) {
        try {
            Cluster cluster = stack.getCluster();
            StackRepoDetails stackRepoDetails = clusterComponentConfigProvider.getHDPRepo(cluster.getId());
            String repoId = stackRepoDetails.getStack().get("repoid");
            if (!repoId.toLowerCase().startsWith("hdf")) {
                Map<String, Map<String, Map<String, String>>> hostGroupConfig = hadoopConfigurationService.getHostGroupConfiguration(blueprintText, hostGroups);
                blueprintText = ambariClient.extendBlueprintHostGroupConfiguration(blueprintText, hostGroupConfig);
                Map<String, Map<String, String>> globalConfig = hadoopConfigurationService.getGlobalConfiguration(blueprintText, hostGroups);
                blueprintText = ambariClient.extendBlueprintGlobalConfiguration(blueprintText, globalConfig);
                blueprintText = hbaseConfigProvider.addHBaseClient(blueprintText);
            } else {
                blueprintText = addHDFConfigToBlueprint(stack, ambariClient, blueprintText, hostGroups);
            }
            if (cluster.isSecure()) {
                blueprintText = kerberosBlueprintService.extendBlueprintWithKerberos(stack, blueprintText, ambariClient);
            }
            LOGGER.info("Adding generated validation to Ambari: {}", JsonUtil.minify(blueprintText));
            ambariClient.addBlueprint(blueprintText, cluster.getTopologyValidation());
        } catch (HttpResponseException hre) {
            if (hre.getStatusCode() == HttpStatus.SC_CONFLICT) {
                LOGGER.info("Ambari validation already exists for stack: {}", stack.getId());
            } else {
                throw new CloudbreakServiceException("Ambari Blueprint could not be added: " + AmbariClientExceptionUtil.getErrorMessage(hre), hre);
            }
        } catch (IOException e) {
            throw new CloudbreakServiceException(e);
        }
    }

    private String addHDFConfigToBlueprint(Stack stack, BlueprintService ambariClient, String blueprintText, Set<HostGroup> hostGroups,
            List<InstanceMetaData> aliveInstancesInInstanceGroup) {
        Set<String> nifiMasters = blueprintProcessor.getHostGroupsWithComponent(blueprintText, "NIFI_MASTER");
        Set<InstanceGroup> nifiIgs = hostGroups.stream().filter(hg -> nifiMasters.contains(hg.getName())).map(hg -> hg.getConstraint()
                .getInstanceGroup()).collect(Collectors.toSet());
        List<String> nifiFqdns = nifiIgs.stream().flatMap(ig -> instanceMetadataRepository.findAliveInstancesInInstanceGroup(ig.getId()).stream())
                .map(InstanceMetaData::getDiscoveryFQDN).collect(Collectors.toList());
        AtomicInteger index = new AtomicInteger(0);
        String nodeIdentities = nifiFqdns.stream()
                .map(fqdn -> String.format("<property name=\"Node Identity %s\">CN=%s, OU=NIFI</property>", index.addAndGet(1), fqdn))
                .collect(Collectors.joining());
        blueprintText = ambariClient.extendBlueprintGlobalConfiguration(blueprintText, ImmutableMap.of("nifi-ambari-ssl-config", ImmutableMap.of(
                "content", nodeIdentities)));
        blueprintText = ambariClient.extendBlueprintGlobalConfiguration(blueprintText, ImmutableMap.of("nifi-ambari-ssl-config", ImmutableMap.of(
                "nifi.initial.admin.identity", stack.getCluster().getUserName())));
        blueprintText = ambariClient.extendBlueprintGlobalConfiguration(blueprintText, ImmutableMap.of("ams-grafana-env", ImmutableMap.of(
                "metrics_grafana_username", stack.getCluster().getUserName(),
                "metrics_grafana_password", stack.getCluster().getPassword())));
        return blueprintText;
    }

    private String extendHiveConfig(BlueprintService ambariClient, String processingBlueprint) {
        Map<String, Map<String, String>> config = new HashMap<>();
        Map<String, String> hiveSite = new HashMap<>();
        hiveSite.put("hive.server2.authentication.kerberos.keytab", "/etc/security/keytabs/hive2.service.keytab");
        config.put("hive-site", hiveSite);
        return ambariClient.extendBlueprintGlobalConfiguration(processingBlueprint, config);
    }

    private Map<String, Integer> installServices(List<String> hosts, Stack stack, AmbariClient ambariClient, String hostGroup) {
        try {
            String blueprintName = stack.getCluster().getBlueprint().getAmbariName();
            // In case If we changed the blueprintName field we need to query the validation name information from ambari
            Map<String, String> blueprintsMap = ambariClient.getBlueprintsMap();
            if (!blueprintsMap.entrySet().isEmpty()) {
                blueprintName = blueprintsMap.keySet().iterator().next();
            }
            return singletonMap("UPSCALE_REQUEST", ambariClient.addHostsWithBlueprint(blueprintName, hostGroup, hosts));
        } catch (HttpResponseException e) {
            if ("Conflict".equals(e.getMessage())) {
                throw new BadRequestException("Host already exists.", e);
            } else {
                String errorMessage = AmbariClientExceptionUtil.getErrorMessage(e);
                throw new CloudbreakServiceException("Ambari could not install services. " + errorMessage, e);
            }
        }
    }

}
