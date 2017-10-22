package com.sequenceiq.cloudbreak.notification;

public interface NotificationSender {
    <T> void send(Notification<T> notification);
}
