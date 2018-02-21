package com.sequenceiq.cloudbreak.service.cluster.api;

import java.util.Collection;

import com.sequenceiq.cloudbreak.core.CloudbreakException;
import com.sequenceiq.cloudbreak.core.CloudbreakSecuritySetupException;
import com.sequenceiq.cloudbreak.domain.HostGroup;
import com.sequenceiq.cloudbreak.domain.HostMetadata;
import com.sequenceiq.cloudbreak.domain.Stack;

public interface ClusterApi {

    void waitForServer(Stack stack) throws CloudbreakException;

    void buildCluster(Stack stack);

    void waitForHosts(Stack stack) throws CloudbreakSecuritySetupException;

    void upscaleCluster(Stack stack, HostGroup hostGroup, Collection<HostMetadata> hostMetadata) throws CloudbreakException;

    void stopCluster(Stack stack) throws CloudbreakException;

    int startCluster(Stack stack) throws CloudbreakException;

    void replaceUserNamePassword(Stack stackId, String newUserName, String newPassword) throws CloudbreakException;

    void updateUserNamePassword(Stack stack, String newPassword) throws CloudbreakException;

    void prepareSecurity(Stack stack);

    void disableSecurity(Stack stack);

    boolean available(Stack stack) throws CloudbreakSecuritySetupException;

    void waitForServices(Stack stack, int requestId) throws CloudbreakException;

}
