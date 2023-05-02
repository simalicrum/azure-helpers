import { StorageManagementClient } from "@azure/arm-storage";
import { DefaultAzureCredential } from "@azure/identity";
import {
  StorageSharedKeyCredential,
  BlobServiceClient,
  BlockBlobClient,
  BlobClient,
  BlobBatchClient,
  BlobSASPermissions,
  generateBlobSASQueryParameters,
  AccountSASPermissions,
  AccountSASServices,
  AccountSASResourceTypes,
  SASProtocol,
  generateAccountSASQueryParameters
} from "@azure/storage-blob";
import fs from "fs";

const streamToText = async (readable) => {
  readable.setEncoding("utf8");
  let data = "";
  for await (const chunk of readable) {
    data += chunk;
  }
  return data;
};

export const storageAccountList = (subscriptionId) => {
  const credential = new DefaultAzureCredential();
  const client = new StorageManagementClient(credential, subscriptionId);
  return client.storageAccounts.list();
};

export const storageAccountListKeys = (
  subscriptionId,
  resourceGroupName,
  accountName
) => {
  const credential = new DefaultAzureCredential();
  const client = new StorageManagementClient(credential, subscriptionId);
  return client.storageAccounts.listKeys(resourceGroupName, accountName);
};

export const createBlobServiceClient = (account, accountKey) => {
  const sharedKeyCredential = new StorageSharedKeyCredential(
    account,
    accountKey
  );
  const blobServiceClient = new BlobServiceClient(
    `https://${account}.blob.core.windows.net`,
    sharedKeyCredential
  );
  return blobServiceClient;
};

export const createBlockBlobClient = (url, accountKey) => {
  const [match, account] = url.match(
    /https:\/\/(\w+)\.blob\.core\.windows\.net/
  );
  const sharedKeyCredential = new StorageSharedKeyCredential(
    account,
    accountKey
  );
  const blockBlobClient = new BlockBlobClient(url, sharedKeyCredential);
  return blockBlobClient;
};

export const createBlobClient = (url, accountKey, options) => {
  const [match, account] = url.match(
    /https:\/\/(\w+)\.blob\.core\.windows\.net/
  );
  const sharedKeyCredential = new StorageSharedKeyCredential(
    account,
    accountKey
  );
  const blobClient = new BlobClient(url, sharedKeyCredential, options);
  return blobClient;
};

export const createBlobBatchClient = (url, accountKey, options) => {
  const [match, account] = url.match(
    /https:\/\/(\w+)\.blob\.core\.windows\.net/
  );
  const sharedKeyCredential = new StorageSharedKeyCredential(
    account,
    accountKey
  );
  const blobClient = new BlobBatchClient(url, sharedKeyCredential, options);
  return blobClient;
};

export const listContainers = (account, accountKey) => {
  const blobServiceClient = createBlobServiceClient(account, accountKey);
  return blobServiceClient.listContainers();
};

export const listBlobsFlat = (account, accountKey, container, options) => {
  const blobServiceClient = createBlobServiceClient(account, accountKey);
  const containerClient = blobServiceClient.getContainerClient(container);
  return containerClient.listBlobsFlat(options);
};

export const listBlobsFlatByUrl = (url, accountKey, options) => {
  const [match, account, container] = url.match(
    /https:\/\/(\w+)\.blob\.core\.windows\.net\/(\w+)\//
  );
  const blobServiceClient = createBlobServiceClient(account, accountKey);
  const containerClient = blobServiceClient.getContainerClient(container);
  return containerClient.listBlobsFlat(options);
};

export const listBlobsByHierarchy = (
  account,
  accountKey,
  container,
  delimiter,
  options
) => {
  const blobServiceClient = createBlobServiceClient(account, accountKey);
  const containerClient = blobServiceClient.getContainerClient(container);
  return containerClient.listBlobsByHierarchy(delimiter, options);
};

export const listBlobsByHierarchybyUrl = (
  url,
  accountKey,
  delimiter,
  options
) => {
  const [match, account, container] = url.match(
    /https:\/\/(\w+)\.blob\.core\.windows\.net\/(\w+)\//
  );
  const blobServiceClient = createBlobServiceClient(account, accountKey);
  const containerClient = blobServiceClient.getContainerClient(container);
  return containerClient.listBlobsByHierarchy(delimiter, options);
};

const defaultMaxConcurrency = 20;
const defaultBlockSize = 4 * 1024 * 1024;

export const copyBlob = async (sourceUrl, destinationUrl, accountKey) => {
  const destinationBlobClient = createBlobClient(destinationUrl, accountKey);
  const copyPoller = await destinationBlobClient.beginCopyFromURL(sourceUrl, {
    intervalInMs: 1000,
  });
  return copyPoller.pollUntilDone();
};

export const readBlob = async (url, accountKey) => {
  const blockBlobClient = createBlockBlobClient(url, accountKey);
  const downloadBlockBlobResponse = await blockBlobClient.download(0);
  return streamToText(downloadBlockBlobResponse.readableStreamBody);
};

export const writeToBlob = async (url, data, accountKey, uploadOptions) => {
  const blockBlobClient = createBlockBlobClient(url, accountKey);
  return blockBlobClient.upload(data, data.length, uploadOptions);
};

export const createBlobFromLocalPath = async (
  url,
  accountKey,
  localFileWithPath,
  uploadOptions,
  progressFn
) => {
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
            },
          }
        );
        return res._response.status;
      }
    }
  } catch (err) {
    if (err.code === "EACCES") {
      return 403;
    } else {
      console.log(err);
      return 500;
    }
  }
};

export const deleteBlob = async (url, accountKey, options) => {
  const blobClient = createBlobClient(url, accountKey);
  return blobClient.delete(options);
};

export const setAccessTier = async (url, accountKey, tier, options) => {
  const blobClient = createBlobClient(url, accountKey);
  return blobClient.setAccessTier(tier, options);
};

export const deleteBlobs = async (urls, accountKey, options) => {
  const url = new URL(urls[0]);
  const blobBatchClient = createBlobBatchClient(url.origin, accountKey);
  const [match, account] = urls[0].match(
    /https:\/\/(\w+)\.blob\.core\.windows\.net/
  );
  const sharedKeyCredential = new StorageSharedKeyCredential(
    account,
    accountKey
  );
  return blobBatchClient.deleteBlobs(urls, sharedKeyCredential, options);
};

export const setBlobsAccessTier = async (urls, accountKey, tier, options) => {
  const url = new URL(urls[0]);
  const blobBatchClient = createBlobBatchClient(url.origin, accountKey);
  const [match, account] = urls[0].match(
    /https:\/\/(\w+)\.blob\.core\.windows\.net/
  );
  const sharedKeyCredential = new StorageSharedKeyCredential(
    account,
    accountKey
  );
  return blobBatchClient.setBlobsAccessTier(
    urls,
    sharedKeyCredential,
    tier,
    options
  );
};

export const getBlobProps = (url, accountKey, options) => {
  const blobClient = createBlobClient(url, accountKey);
  return blobClient.getProperties(options);
};

export const storageAccountsListProps = async (subscriptionId) => {
  let storageAccounts = [];
  let promises = [];
  for await (const storageAccount of storageAccountList(subscriptionId)) {
    const [match, resourceGroup] = storageAccount.id.match(
      /\/resourceGroups\/(.*?)\//
    );
    storageAccounts.push({ ...storageAccount, resourceGroup });
    promises.push(
      storageAccountListKeys(subscriptionId, resourceGroup, storageAccount.name)
    );
  }
  const keys = await Promise.all(promises);
  storageAccounts = storageAccounts.map((element, index) => ({
    ...element,
    keys: keys[index].keys,
  }));
  return storageAccounts;
};

export const createContainerClient = (container, account, accountKey) => {
  const blobServiceClient = createBlobServiceClient(account, accountKey);
  return blobServiceClient.getContainerClient(container);
};

export const getBlobSasUri = (
  blobName,
  container,
  account,
  accountKey,
  storedPolicyName
) => {
  const sharedKeyCredential = new StorageSharedKeyCredential(
    account,
    accountKey
  );
  const containerClient = createContainerClient(container, account, accountKey);
  const sasOptions = {
    containerName: containerClient.containerName,
    blobName: blobName,
  };

  if (storedPolicyName == null) {
    sasOptions.startsOn = new Date();
    sasOptions.expiresOn = new Date(
      new Date().valueOf() + 90 * 24 * 3600 * 1000
    ); // 90 days SaS token expires
    sasOptions.permissions = BlobSASPermissions.parse("r");
  } else {
    sasOptions.identifier = storedPolicyName;
  }

  const sasToken = generateBlobSASQueryParameters(
    sasOptions,
    sharedKeyCredential
  ).toString();

  return `${containerClient.getBlockBlobClient(blobName).url}?${sasToken}`;
};

export const createUrl = (path, account, container) =>
  `https://${account}.blob.core.windows.net/${container}/${encodeURIComponent(
    path
  )}`;

export const createAccountSas = (account, accountKey) => {
  const sharedKeyCredential = new StorageSharedKeyCredential(
    account,
    accountKey
  );
  const sasOptions = {
    services: AccountSASServices.parse("btqf").toString(),          // blobs, tables, queues, files
    resourceTypes: AccountSASResourceTypes.parse("sco").toString(), // service, container, object
    permissions: AccountSASPermissions.parse("rwdlacupi"),          // permissions
    protocol: SASProtocol.Https,
    startsOn: new Date(),
    expiresOn: new Date(new Date().valueOf() + (90 * 24 * 3600 * 1000)),   // 90 days
  };

  const sasToken = generateAccountSASQueryParameters(
    sasOptions,
    sharedKeyCredential
  ).toString();

  // prepend sasToken with `?`
  return (sasToken[0] === '?') ? sasToken : `?${sasToken}`;
}

export const parseStorageAccountId = (id) => {
  const [match, subscriptionId, resourceGroup, account] = id.match(
    /\/subscriptions\/(.*?)\/resourceGroups\/(.*?)\/providers\/Microsoft\.Storage\/storageAccounts\/(.*?)$/
  );
  return { subscriptionId, resourceGroup, account };
};

export const parseStorageTriggerSubject = (subject) => {
  const [match, container, blob] = subject.match(
    /\/blobServices\/default\/containers\/(.*?)\/blobs\/(.*)$/
  );
  return { container, blob };
};