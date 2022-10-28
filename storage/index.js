import { StorageManagementClient } from "@azure/arm-storage";
import { DefaultAzureCredential } from "@azure/identity";
import { StorageSharedKeyCredential, BlobServiceClient, BlockBlobClient } from "@azure/storage-blob";
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

export const listContainers = (account, accountKey) => {
  const blobServiceClient = createBlobServiceClient(account, accountKey);
  return blobServiceClient.listContainers();
}

export const listBlobsFlat = (account, accountKey, container) => {
  const blobServiceClient = createBlobServiceClient(account, accountKey);
  const containerClient = blobServiceClient.getContainerClient(container);
  return containerClient.listBlobsFlat();
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