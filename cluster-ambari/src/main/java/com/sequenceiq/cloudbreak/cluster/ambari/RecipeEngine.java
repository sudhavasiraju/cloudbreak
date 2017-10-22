package com.sequenceiq.cloudbreak.cluster.ambari;

import static com.sequenceiq.cloudbreak.common.type.OrchestratorConstants.SALT;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.google.common.collect.Sets;
import com.sequenceiq.cloudbreak.CloudbreakException;
import com.sequenceiq.cloudbreak.api.model.ExecutionType;
import com.sequenceiq.cloudbreak.api.model.ExecutorType;
import com.sequenceiq.cloudbreak.api.model.FileSystemConfiguration;
import com.sequenceiq.cloudbreak.api.model.FileSystemType;
import com.sequenceiq.cloudbreak.api.model.InstanceGroupType;
import com.sequenceiq.cloudbreak.cluster.ClusterLifecycleEvent;
import com.sequenceiq.cloudbreak.cluster.FileSystemConfigurator;
import com.sequenceiq.cloudbreak.cluster.RecipeScript;
import com.sequenceiq.cloudbreak.cluster.ambari.blueprint.BlueprintProcessor;
import com.sequenceiq.cloudbreak.cluster.recipe.OrchestratorRecipeExecutor;
import com.sequenceiq.cloudbreak.cluster.recipe.RecipeBuilder;
import com.sequenceiq.cloudbreak.domain.Cluster;
import com.sequenceiq.cloudbreak.domain.Credential;
import com.sequenceiq.cloudbreak.domain.FileSystem;
import com.sequenceiq.cloudbreak.domain.HostGroup;
import com.sequenceiq.cloudbreak.domain.Recipe;
import com.sequenceiq.cloudbreak.domain.Stack;
import com.sequenceiq.cloudbreak.orchestrator.host.HostOrchestrator;
import com.sequenceiq.cloudbreak.util.FileReaderUtils;
import com.sequenceiq.cloudbreak.util.JsonUtil;

@Component
public class RecipeEngine {

    private static final Logger LOGGER = LoggerFactory.getLogger(RecipeEngine.class);

    @Resource
    private Map<FileSystemType, FileSystemConfigurator> fileSystemConfigurators;

    @Inject
    private RecipeBuilder recipeBuilder;

    @Inject
    private OrchestratorRecipeExecutor orchestratorRecipeExecutor;

    @Inject
    private BlueprintProcessor blueprintProcessor;

    public void executePreInstall(Stack stack, Set<HostGroup> hostGroups, Optional<HostOrchestrator> orchestrator, boolean smartSenseIsConfigurable)
            throws CloudbreakException {
        if (orchestrator.isPresent()) {
            addFsRecipes(stack, hostGroups);
            addSmartSenseRecipe(stack, hostGroups, smartSenseIsConfigurable);
            addContainerExecutorScripts(stack, hostGroups);
            boolean recipesFound = recipesFound(hostGroups);
            if (recipesFound) {

                orchestratorRecipeExecutor.uploadRecipes(stack, hostGroups, orchestrator.get());
                orchestratorRecipeExecutor.preInstall(stack,orchestrator.get());
            }
        }
    }

    public void executeUpscalePreInstall(Stack stack, HostGroup hostGroup, Set<HostGroup> hostGroups, Optional<HostOrchestrator> orchestrator) throws CloudbreakException {
        if (orchestrator.isPresent()) {
            Set<HostGroup> hgs = Collections.singleton(hostGroup);
            addFsRecipes(stack, hgs);
            boolean recipesFound = recipesFound(hgs);
            if (recipesFound) {
                if (hostGroup.getConstraint().getInstanceGroup().getInstanceGroupType() == InstanceGroupType.GATEWAY) {
                    orchestratorRecipeExecutor.uploadRecipes(stack, hostGroups, orchestrator.get());
                }
                orchestratorRecipeExecutor.preInstall(stack, orchestrator.get());
            }
        }
    }

    public void executePostInstall(Stack stack, Optional<HostOrchestrator> orchestrator) throws CloudbreakException {
        if (orchestrator.isPresent()) {
            orchestratorRecipeExecutor.postInstall(stack, orchestrator.get());
        }
    }

    public void executeUpscalePostInstall(Stack stack, Optional<HostOrchestrator> orchestrator) throws CloudbreakException {
        if (orchestrator.isPresent()) {
            orchestratorRecipeExecutor.postInstall(stack, orchestrator.get());
        }
    }

    private void addFsRecipes(Stack stack, Set<HostGroup> hostGroups) throws CloudbreakException {
        String orchestrator = stack.getOrchestrator().getType();
        if (SALT.equals(orchestrator)) {
            Cluster cluster = stack.getCluster();
            String blueprintText = cluster.getBlueprint().getBlueprintText();
            FileSystem fs = cluster.getFileSystem();
            if (fs != null) {
                try {
                    addFsRecipesToHostGroups(stack.getCredential(), hostGroups, blueprintText, fs);
                } catch (IOException e) {
                    throw new CloudbreakException("can not add FS recipes to host groups", e);
                }
            }
            addHDFSRecipe(cluster, blueprintText, hostGroups);
        }
    }

    private void addContainerExecutorScripts(Stack stack, Set<HostGroup> hostGroups) {
        try {
            Cluster cluster = stack.getCluster();
            if (cluster != null && ExecutorType.CONTAINER.equals(cluster.getExecutorType())) {
                for (HostGroup hostGroup : hostGroups) {
                    String script = FileReaderUtils.readFileFromClasspath("scripts/configure-container-executor.sh");
                    RecipeScript recipeScript = new RecipeScript(script, ClusterLifecycleEvent.POST_INSTALL);
                    Recipe recipe = recipeBuilder.buildRecipes("configure-container-executor",
                            Collections.singletonList(recipeScript)).get(0);
                    hostGroup.addRecipe(recipe);
                }
            }
        } catch (IOException e) {
            LOGGER.warn("Cannot configure container executor", e);
        }
    }

    private void addFsRecipesToHostGroups(Credential credential, Set<HostGroup> hostGroups, String blueprintText, FileSystem fs) throws IOException {
        String scriptName = fs.getType().toLowerCase();
        FileSystemConfigurator fsConfigurator = fileSystemConfigurators.get(FileSystemType.valueOf(fs.getType()));
        FileSystemConfiguration fsConfiguration = getFileSystemConfiguration(fs);
        List<RecipeScript> recipeScripts = fsConfigurator.getScripts(credential, fsConfiguration);
        List<Recipe> fsRecipes = recipeBuilder.buildRecipes(scriptName, recipeScripts);
        for (int i = 0; i < fsRecipes.size(); i++) {
            RecipeScript recipeScript = recipeScripts.get(i);
            Recipe recipe = fsRecipes.get(i);
            for (HostGroup hostGroup : hostGroups) {
                if (ExecutionType.ALL_NODES == recipeScript.getExecutionType()) {
                    hostGroup.addRecipe(recipe);
                } else if (ExecutionType.ONE_NODE == recipeScript.getExecutionType() && isComponentPresent(blueprintText, "NAMENODE", hostGroup)) {
                    hostGroup.addRecipe(recipe);
                    break;
                }
            }
        }
    }

    private FileSystemConfiguration getFileSystemConfiguration(FileSystem fs) throws IOException {
        String json = JsonUtil.writeValueAsString(fs.getProperties());
        return (FileSystemConfiguration) JsonUtil.readValue(json, FileSystemType.valueOf(fs.getType()).getClazz());
    }

    private boolean recipesFound(Set<HostGroup> hostGroups) {
        for (HostGroup hostGroup : hostGroups) {
            if (!hostGroup.getRecipes().isEmpty()) {
                return true;
            }
        }
        return false;
    }

    private void addHDFSRecipe(Cluster cluster, String blueprintText, Set<HostGroup> hostGroups) {
        try {
            for (HostGroup hostGroup : hostGroups) {
                if (isComponentPresent(blueprintText, "NAMENODE", hostGroup)) {
                    String script = FileReaderUtils.readFileFromClasspath("scripts/hdfs-home.sh").replaceAll("\\$USER", cluster.getUserName());
                    RecipeScript recipeScript = new RecipeScript(script, ClusterLifecycleEvent.POST_INSTALL);
                    Recipe recipe = recipeBuilder.buildRecipes("hdfs-home", Collections.singletonList(recipeScript)).get(0);
                    hostGroup.addRecipe(recipe);
                    break;
                }
            }
        } catch (IOException e) {
            LOGGER.warn("Cannot create HDFS home dir recipe", e);
        }
    }

    private void addSmartSenseRecipe(Stack stack, Set<HostGroup> hostGroups, boolean smartSenseIsConfigurable) {
        try {
            Cluster cluster = stack.getCluster();
            String blueprintText = cluster.getBlueprint().getBlueprintText();
            if (smartSenseIsConfigurable) {
                for (HostGroup hostGroup : hostGroups) {
                    if (isComponentPresent(blueprintText, "HST_AGENT", hostGroup)) {
                        String script = FileReaderUtils.readFileFromClasspath("scripts/smartsense-capture-schedule.sh");
                        RecipeScript recipeScript = new RecipeScript(script, ClusterLifecycleEvent.POST_INSTALL);
                        Recipe recipe = recipeBuilder.buildRecipes("smartsense-capture-schedule",
                                Collections.singletonList(recipeScript)).get(0);
                        hostGroup.addRecipe(recipe);
                        break;
                    }
                }
            }
        } catch (IOException e) {
            LOGGER.warn("Cannot create SmartSense caputre schedule setter recipe", e);
        }
    }

    private boolean isComponentPresent(String blueprint, String component, HostGroup hostGroup) {
        return isComponentPresent(blueprint, component, Sets.newHashSet(hostGroup));
    }

    private boolean isComponentPresent(String blueprint, String component, Set<HostGroup> hostGroups) {
        for (HostGroup hostGroup : hostGroups) {
            Set<String> components = blueprintProcessor.getComponentsInHostGroup(blueprint, hostGroup.getName());
            if (components.contains(component)) {
                return true;
            }
        }
        return false;
    }
}
