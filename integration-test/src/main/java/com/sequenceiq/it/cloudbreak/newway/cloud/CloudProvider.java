package com.sequenceiq.it.cloudbreak.newway.cloud;

import com.sequenceiq.cloudbreak.api.model.v2.AmbariV2Request;
import com.sequenceiq.it.cloudbreak.newway.CredentialEntity;
import com.sequenceiq.it.cloudbreak.newway.Entity;
import com.sequenceiq.it.cloudbreak.newway.StackEntity;

import java.util.Set;

public abstract class CloudProvider {
    public abstract StackEntity aValidStackRequest();

    public abstract CredentialEntity aValidCredential();

    public abstract AmbariV2Request ambariRequestWithBlueprintId(Long id);

    public abstract Entity aValidStackIsCreated();

    public abstract AmbariV2Request ambariRequestWithBlueprintName(String blueprintHdp26EdwanalyticsName);

    public abstract String getClusterDefaultName();

    public abstract String getPlatform();

    public abstract String getCredentialName();

    public abstract Set<String> regionExpected();

    public abstract String region();
}
