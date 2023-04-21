import { EventGridManagementClient } from "@azure/arm-eventgrid";
import { DefaultAzureCredential } from "@azure/identity";

export const createEventGridManagementClient = (subscriptionId) => {
  const credential = new DefaultAzureCredential();
  const client = new EventGridManagementClient(credential, subscriptionId);
  return client;
};

export const beginCreateOrUpdateAndWaitEventSubscription = (
  subscriptionId,
  scope,
  eventSubscriptionName,
  eventSubscriptionInfo
) => {
  const client = createEventGridManagementClient(subscriptionId);
  return client.eventSubscriptions.beginCreateOrUpdateAndWait(
    scope,
    eventSubscriptionName,
    eventSubscriptionInfo
  );
};

export const beginCreateOrUpdateAndWaitSystemTopic = (
  subscriptionId,
  resourceGroupName,
  systemTopicName,
  systemTopicInfo
) => {
  const client = createEventGridManagementClient(subscriptionId);
  return client.systemTopics.beginCreateOrUpdateAndWait(
    resourceGroupName,
    systemTopicName,
    systemTopicInfo
  );
};

export const systemTopicsList = (subscriptionId) => {
  const client = createEventGridManagementClient(subscriptionId);
  return client.systemTopics.listBySubscription();
};

export const topicEventSubscriptionsList = (
  subscriptionId,
  resourceGroupName,
  topicName
) => {
  const client = createEventGridManagementClient(subscriptionId);
  return client.topicEventSubscriptions.list(resourceGroupName, topicName);
};

export const getSystemTopics = (
  subscriptionId,
  resourceGroupName,
  systemTopicName
) => {
  const client = createEventGridManagementClient(subscriptionId);
  return client.systemTopics.get(resourceGroupName, systemTopicName);
};

export const getSystemTopicEventSubscriptions = (
  subscriptionId,
  resourceGroupName,
  systemTopicName,
  eventSubscriptionName
) => {
  const client = createEventGridManagementClient(subscriptionId);
  return client.systemTopicEventSubscriptions.get(
    resourceGroupName,
    systemTopicName,
    eventSubscriptionName
  );
};
