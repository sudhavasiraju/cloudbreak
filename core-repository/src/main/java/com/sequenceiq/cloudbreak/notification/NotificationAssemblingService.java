package com.sequenceiq.cloudbreak.notification;

import org.springframework.stereotype.Component;

@Component
public class NotificationAssemblingService<T> {
    public Notification<T> createNotification(T notification) {
        return new Notification(notification);
    }
}
