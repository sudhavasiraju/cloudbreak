package com.sequenceiq.it.cloudbreak.recovery;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.openstack4j.api.OSClient;
import org.openstack4j.api.compute.ServerService;
import org.openstack4j.openstack.OSFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.sequenceiq.cloudbreak.api.model.InstanceGroupResponse;
import com.sequenceiq.cloudbreak.api.model.InstanceMetaDataJson;
import com.sequenceiq.cloudbreak.api.model.StackResponse;


public class RecoveryUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(com.sequenceiq.it.cloudbreak.recovery.RecoveryUtil.class);

    protected RecoveryUtil() {

    }

    public static String getInstanceId(StackResponse stackResponse, String hostGroup) {
        String instanceId = null;
        List<InstanceGroupResponse> instanceGroups = stackResponse.getInstanceGroups();

        outerloop:
        for (InstanceGroupResponse instanceGroup : instanceGroups) {
            if (hostGroup.equals(instanceGroup.getGroup().toString())) {
                Set<InstanceMetaDataJson> instanceMetaData = instanceGroup.getMetadata();
                for (InstanceMetaDataJson metaData : instanceMetaData) {
                    instanceId = metaData.getInstanceId();
                    break outerloop;
                }
            }
        }
        Assert.assertNotNull(instanceId);
    return instanceId;
    }

    public static void deleteAWSInstance(Regions region, String instanceId) {
        List<String> idList = new ArrayList<>();
        idList.add(instanceId);

        AmazonEC2Client ec2 = new AmazonEC2Client();
        ec2.setRegion(Region.getRegion(region));

        TerminateInstancesRequest terminateInstancesRequest = new TerminateInstancesRequest(idList);
        ec2.terminateInstances(terminateInstancesRequest);
        LOGGER.info("Instance was deleted with id:" + instanceId);
    }

    public static void deleteOpenstackInstance(String endpoint, String userName, String password, String tenantName, String instanceId) {
        OSClient os = OSFactory.builder()
                .endpoint(endpoint)
                .credentials(userName, password)
                .tenantName(tenantName)
                .authenticate();

        ServerService servers =  os.compute().servers();
        servers.delete(instanceId);
        LOGGER.info("Instance was deleted with id: " + instanceId);
    }
}