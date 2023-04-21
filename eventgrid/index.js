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

export const listBySubscriptionSystemTopics = async (subscriptionId) => {
  const client = createEventGridManagementClient(subscriptionId);
  const resArray = new Array();
  for await (let item of client.systemTopics.listBySubscription()) {
    resArray.push(item);
  }
  return resArray;
};

export const listTopicEventSubscriptions = async (
  subscriptionId,
  resourceGroupName,
  topicName
) => {
  const client = createEventGridManagementClient(subscriptionId);
  const resArray = new Array();
  for await (let item of client.topicEventSubscriptions.list(
    resourceGroupName,
    topicName
  )) {
    resArray.push(item);
  }
  return resArray;
};
