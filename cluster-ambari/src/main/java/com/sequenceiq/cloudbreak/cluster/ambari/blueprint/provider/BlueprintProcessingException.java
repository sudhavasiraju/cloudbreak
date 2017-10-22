package com.sequenceiq.cloudbreak.cluster.ambari.blueprint.provider;


public class BlueprintProcessingException extends RuntimeException {

    public BlueprintProcessingException(String message) {
        super(message);
    }

    public BlueprintProcessingException(String message, Throwable cause) {
        super(message, cause);
    }
}
