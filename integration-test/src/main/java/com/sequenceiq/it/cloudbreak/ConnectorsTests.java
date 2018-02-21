package com.sequenceiq.it.cloudbreak;

import com.sequenceiq.it.cloudbreak.newway.CloudbreakClient;
import com.sequenceiq.it.cloudbreak.newway.CloudbreakTest;
import com.sequenceiq.it.cloudbreak.newway.Credential;
import com.sequenceiq.it.cloudbreak.newway.Region;
import com.sequenceiq.it.cloudbreak.newway.TestParameter;
import com.sequenceiq.it.cloudbreak.newway.cloud.CloudProvider;
import com.sequenceiq.it.cloudbreak.newway.cloud.CloudProviderHelper;
import com.sequenceiq.it.cloudbreak.newway.cloud.OpenstackCloudProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import javax.ws.rs.ForbiddenException;

public class ConnectorsTests extends CloudbreakTest {

    public static final String VALID_CRED_NAME = "valid-credential";

    public static final String CRED_DESCRIPTION = "temporary credential for API E2E tests";

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectorsTests.class);

    private String errorMessage = "";

    private CloudProvider cloudProvider;

    public ConnectorsTests() {
    }

    public ConnectorsTests(CloudProvider cp, TestParameter tp) {
        this.cloudProvider = cp;
        setTestParameter(tp);
    }

    @BeforeTest
    @Parameters({ "provider" })
    public void beforeTest(@Optional(OpenstackCloudProvider.OPENSTACK) String provider) {
        LOGGER.info("before cluster test set provider: " + provider);
        if (this.cloudProvider != null) {
            LOGGER.info("cloud provider already set - running from factory test");
            return;
        }
        this.cloudProvider = CloudProviderHelper.providerFactory(provider, getTestParameter())[0];
    }

    @AfterTest
    public void cleanUp() throws Exception {
        LOGGER.info("Delete credential: \'{}\'", cloudProvider.getCredentialName());

        try {
            given(CloudbreakClient.isCreated());
            given(Credential.request()
                    .withName(cloudProvider.getCredentialName()));
            when(Credential.delete());
        } catch (ForbiddenException e) {
            String exceptionMessage = e.getResponse().readEntity(String.class);
            this.errorMessage = exceptionMessage.substring(exceptionMessage.lastIndexOf(":") + 1);
            LOGGER.info("ForbiddenException message ::: " + this.errorMessage);
        }
    }

    @Test(priority = 0, groups = { "common" })
    public void testCreateValidCredential() throws Exception {
        given(CloudbreakClient.isCreated());
        given(cloudProvider.aValidCredential(), cloudProvider.getPlatform() + " credential is created");
        given(Region.request(), cloudProvider.getPlatform() + " region request");
        when(Region.getPlatformRegions(), "Regions are requested to "
                + cloudProvider.getPlatform() + " credential");
        then(Region.assertThis(
                (region, t) -> {
                    Assert.assertTrue(region.getRegionResponse().getRegions().contains(cloudProvider.region()),
                            cloudProvider.region() + " region is not found in the response");
                }), cloudProvider.region() + " region should be part of the response."
        );
    }
}
