package com.sequenceiq.cloudbreak.task;

public abstract class SimpleStatusCheckerTask<T> implements StatusCheckerTask<T> {

    @Override
    public void handleException(Exception e) {
        throw new CloudbreakServiceException(e);
    }

}
