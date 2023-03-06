import {
  BatchServiceClient,
  BatchSharedKeyCredentials
} from "@azure/batch";

export const createBatchServiceClient = (url, account, accountKey) => {
  const sharedKeyCredential = new BatchSharedKeyCredentials(account, accountKey);
  const batchServiceClient = new BatchServiceClient(sharedKeyCredential, url);
  return batchServiceClient;
}