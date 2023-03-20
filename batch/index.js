import {
  BatchServiceClient,
  BatchSharedKeyCredentials
} from "@azure/batch";

export const createBatchServiceClient = (url, account, accountKey) => {
  const sharedKeyCredential = new BatchSharedKeyCredentials(account, accountKey);
  const batchServiceClient = new BatchServiceClient(sharedKeyCredential, url);
  return batchServiceClient;
}

export const existsBatchPool = (url, account, accountKey, poolId) => {
  const batchServiceClient = createBatchServiceClient(url, account, accountKey);
  return batchServiceClient.pool.exists(poolId);
}

export const getBatchPool = (url, account, accountKey, poolId) => {
  const batchServiceClient = createBatchServiceClient(url, account, accountKey);
  return batchServiceClient.pool.get(poolId);
}

export const addBatchJob = (url, account, accountKey, jobConfig) => {
  const batchServiceClient = createBatchServiceClient(url, account, accountKey);
  return batchServiceClient.job.add(jobConfig);
}

export const addBatchTask = (url, account, accountKey, jobId, taskConfig) => {
  const batchServiceClient = createBatchServiceClient(url, account, accountKey);
  return batchServiceClient.task.add(jobId, taskConfig);
}