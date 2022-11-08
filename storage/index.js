import { StorageManagementClient } from "@azure/arm-storage";
import { DefaultAzureCredential } from "@azure/identity";
import { StorageSharedKeyCredential, BlobServiceClient, BlockBlobClient, BlobClient, BlobBatchClient } from "@azure/storage-blob";
import fs from 'fs';

export const storageAccountList = async (subscriptionId) => {
  const credential = new DefaultAzureCredential();
  const client = new StorageManagementClient(credential, subscriptionId);
  const resArray = new Array();
  for await (let item of client.storageAccounts.list()) {
    resArray.push(item);
  }
  return resArray;
}

export const storageAccountListKeys = async (subscriptionId, resourceGroupName, accountName) => {
  const credential = new DefaultAzureCredential();
  const client = new StorageManagementClient(credential, subscriptionId);
  const result = await client.storageAccounts.listKeys(resourceGroupName, accountName);
  return result;
}

export const createBlobServiceClient = (account, accountKey) => {
  const sharedKeyCredential = new StorageSharedKeyCredential(account, accountKey);
  const blobServiceClient = new BlobServiceClient(
    `https://${account}.blob.core.windows.net`,
    sharedKeyCredential);
  return blobServiceClient;
}

export const createBlockBlobClient = (url, accountKey) => {
  const [match, account] = url.match(/https:\/\/(\w+)\.blob\.core\.windows\.net/);
  const sharedKeyCredential = new StorageSharedKeyCredential(account, accountKey);
  const blockBlobClient = new BlockBlobClient(url, sharedKeyCredential);
  return blockBlobClient;
}

export const createBlobClient = (url, accountKey, options) => {
  const [match, account] = url.match(/https:\/\/(\w+)\.blob\.core\.windows\.net/);
  const sharedKeyCredential = new StorageSharedKeyCredential(account, accountKey);
  const blobClient = new BlobClient(url, sharedKeyCredential, options);
  return blobClient;
}

export const createBlobBatchClient = (url, accountKey, options) => {
  const [match, account] = url.match(/https:\/\/(\w+)\.blob\.core\.windows\.net/);
  const sharedKeyCredential = new StorageSharedKeyCredential(account, accountKey);
  const blobClient = new BlobBatchClient(url, sharedKeyCredential, options);
  return blobClient;
}

export const listContainers = (account, accountKey) => {
  const blobServiceClient = createBlobServiceClient(account, accountKey);
  return blobServiceClient.listContainers();
}

export const listBlobsFlat = (account, accountKey, container, options) => {
  const blobServiceClient = createBlobServiceClient(account, accountKey);
  const containerClient = blobServiceClient.getContainerClient(container);
  return containerClient.listBlobsFlat(options);
}

export const listBlobsFlatByUrl = (url, accountKey, options) => {
  const [match, account, container] = url.match(/https:\/\/(\w+)\.blob\.core\.windows\.net\/(\w+)\//);
  const blobServiceClient = createBlobServiceClient(account, accountKey);
  const containerClient = blobServiceClient.getContainerClient(container);
  return containerClient.listBlobsFlat(options);
}

export const listBlobsByHierarchy = (account, accountKey, container, delimiter, options) => {
  const blobServiceClient = createBlobServiceClient(account, accountKey);
  const containerClient = blobServiceClient.getContainerClient(container);
  return containerClient.listBlobsByHierarchy(delimiter, options);
}

export const listBlobsByHierarchybyUrl = (url, accountKey, delimiter, options) => {
  const [match, account, container] = url.match(/https:\/\/(\w+)\.blob\.core\.windows\.net\/(\w+)\//);
  const blobServiceClient = createBlobServiceClient(account, accountKey);
  const containerClient = blobServiceClient.getContainerClient(container);
  return containerClient.listBlobsByHierarchy(delimiter, options);
}

const defaultMaxConcurrency = 20;
const defaultBlockSize = 4 * 1024 * 1024;

export const createBlobFromLocalPath = async (url, accountKey, localFileWithPath, uploadOptions, progressFn) => {
  try {
    const { highWaterMark, blockSize, maxConcurrency } = uploadOptions;
    const blockBlobClient = createBlockBlobClient(url, accountKey);
    // Get source file size in bytes
    const fileStats = fs.statSync(localFileWithPath);
    // Get destination blob file size in bytes.
    try {
      const blobProps = await blockBlobClient.getProperties();
      if (fileStats.size === blobProps.contentLength) {
        // File is already in destination so return success code
        return 200;
      }
    } catch (err) {
      if (err.statusCode === 404) {
        // Blob was not found in destination so upload file to blob storage
        const res = await blockBlobClient.uploadStream(
          fs.createReadStream(localFileWithPath, {
            highWaterMark: blockSize || defaultBlockSize,
          }),
          blockSize || defaultBlockSize,
          maxConcurrency || defaultMaxConcurrency,
          {
            onProgress: (ev) => {
              progressFn(ev, fileStats.size);
            }
          }
        );
        return res._response.status;
      }
    }
  } catch (err) {
    if (err.code === 'EACCES') {
      return 403
    } else {
      console.log(err);
      return 500
    }

  }
}

export const deleteBlob = async (url, accountKey, options) => {
  const blobClient = createBlobClient(url, accountKey);
  return blobClient.delete(options);
}

export const setAccessTier = async (url, accountKey, tier, options) => {
  const blobClient = createBlobClient(url, accountKey);
  return blobClient.setAccessTier(tier, options);
}

export const deleteBlobs = async (urls, accountKey, options) => {
  const url = new URL(urls[0]);
  const blobBatchClient = createBlobBatchClient(url.origin, accountKey);
  const [match, account] = urls[0].match(/https:\/\/(\w+)\.blob\.core\.windows\.net/);
  const sharedKeyCredential = new StorageSharedKeyCredential(account, accountKey);
  return blobBatchClient.deleteBlobs(urls, sharedKeyCredential, options);
}


export const setBlobsAccessTier = async (urls, accountKey, tier, options) => {
  const url = new URL(urls[0]);
  const blobBatchClient = createBlobBatchClient(url.origin, accountKey);
  const [match, account] = urls[0].match(/https:\/\/(\w+)\.blob\.core\.windows\.net/);
  const sharedKeyCredential = new StorageSharedKeyCredential(account, accountKey);
  return blobBatchClient.setBlobsAccessTier(urls, sharedKeyCredential, tier, options);
}

export const storageAccountsListProps = async (subscriptionId) => {
  const storageAccounts = await storageAccountList(subscriptionId);
  for (const [i, storageAccount] of storageAccounts.entries()) {
    const [match, resourceGroup] = storageAccount.id.match(/\/resourceGroups\/(.*?)\//);
    const keys = await storageAccountListKeys(process.env.AZURE_SUBSCRIPTION_ID, resourceGroup, storageAccount.name);
    storageAccounts[i].keys = keys.keys;
    storageAccounts[i].resourceGroup = resourceGroup;
    storageAccounts[i].containers = [];
  }
  return storageAccounts;
}