package com.sequenceiq.cloudbreak.service.cluster.flow;

import com.sequenceiq.cloudbreak.domain.Stack;
import com.sequenceiq.cloudbreak.context.StackContext;

public class ImageCheckerContext extends StackContext {

    public ImageCheckerContext(Stack stack) {
        super(stack);
    }
}
