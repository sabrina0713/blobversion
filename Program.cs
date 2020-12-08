using System;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using System.IO;
using System.Threading.Tasks;
using Azure.Storage.Blobs.Specialized;
using Azure;
using System.Collections.Generic;
using System.Text;
namespace BlobQuickstartV12
{
    class Program
    {
        static async Task Main()
        {
            
            string connectionString="";
            Console.WriteLine("Azure Blob storage v12 - .NET quickstart sample\n");
            //string connectionString = Environment.GetEnvironmentVariable("AZURE_STORAGE_CONNECTION_STRING");
            BlobServiceClient blobServiceClient = new BlobServiceClient(connectionString);
            string containerName = "quickstartblobs" + Guid.NewGuid().ToString();
            BlobContainerClient containerClient = await blobServiceClient.CreateBlobContainerAsync(containerName);
            string localPath = "./data/";
            string fileName = "quickstart" + Guid.NewGuid().ToString() + ".txt";
            string localFilePath = Path.Combine(localPath, fileName);
            await File.WriteAllTextAsync(localFilePath, "Hello, World!");
            BlobClient blobClient = containerClient.GetBlobClient(fileName);

          
            Console.WriteLine("Uploading to Blob storage as blob:\n\t {0}\n", blobClient.Uri);
            using FileStream uploadFileStream = File.OpenRead(localFilePath);
            await blobClient.UploadAsync(uploadFileStream, true);
            //await UpdateVersionedBlobMetadata(containerClient,fileName);
            //get the version of a specific container
             string containerNameVersion = "quickstartblobs2a0bfb52-1d0b-4c10-9479-6d9566d240f6";
            BlobContainerClient containerClientVersions = blobServiceClient.GetBlobContainerClient(containerNameVersion);
            await ListBlobVersions(containerClientVersions,50 );
            //await UpdateVersionedBlobMetadata(containerClientVersions,"quickstart66ff3e47-7c98-436e-90cd-81734b087e6f.txt");
            //await ListBlobVersions(containerClient,50 );
            uploadFileStream.Close();
            Console.WriteLine("Listing blobs...");
           /* await foreach (BlobItem blobItem in containerClient.GetBlobsAsync())
            {
                Console.WriteLine("\t" + blobItem.Name);
            }
            string downloadFilePath = localFilePath.Replace(".txt", "DOWNLOADED.txt");
            Console.WriteLine("\nDownloading blob to\n\t{0}\n", downloadFilePath);
            BlobDownloadInfo download = await blobClient.DownloadAsync();
            using (FileStream downloadFileStream = File.OpenWrite(downloadFilePath))
            {
                await download.Content.CopyToAsync(downloadFileStream);
                downloadFileStream.Close();
            }*/
            

        }
        public static async Task UpdateVersionedBlobMetadata(BlobContainerClient blobContainerClient, 
                                                     string blobName)
    {
        try
        {
            // Create the container.
            await blobContainerClient.CreateIfNotExistsAsync();

            // Upload a block blob.
            
            BlockBlobClient blockBlobClient = blobContainerClient.GetBlockBlobClient(blobName);

            string blobContents = string.Format("Block blob created at {0}.", DateTime.Now);
            byte[] byteArray = Encoding.ASCII.GetBytes(blobContents);

            string initialVersionId;
            using (MemoryStream stream = new MemoryStream(byteArray))
            {
                Response<BlobContentInfo> uploadResponse = 
                    await blockBlobClient.UploadAsync(stream, null, default);

                // Get the version ID for the current version.
                initialVersionId = uploadResponse.Value.VersionId;
                
            }
            
            // Update the blob's metadata to trigger the creation of a new version.
            Dictionary<string, string> metadata = new Dictionary<string, string>
            {
                { "key", "value" },
                { "key1", "value1" }
            };

            Response<BlobInfo> metadataResponse = 
                await blockBlobClient.SetMetadataAsync(metadata);

            // Get the version ID for the new current version.
            string newVersionId = metadataResponse.Value.VersionId;

            // Request metadata on the previous version.
            BlockBlobClient initialVersionBlob = blockBlobClient.WithVersion(initialVersionId);
            Response<BlobProperties> propertiesResponse = await initialVersionBlob.GetPropertiesAsync();
            PrintMetadata(propertiesResponse);

            // Request metadata on the current version.
            BlockBlobClient newVersionBlob = blockBlobClient.WithVersion(newVersionId);
            Response<BlobProperties> newPropertiesResponse = await newVersionBlob.GetPropertiesAsync();
            PrintMetadata(newPropertiesResponse);
        }
        catch (RequestFailedException e)
        {
            Console.WriteLine(e.Message);
            Console.ReadLine();
            throw;
        }
    }

    static void PrintMetadata(Response<BlobProperties> propertiesResponse)
    {
        if (propertiesResponse.Value.Metadata.Count > 0)
        {
            Console.WriteLine("Metadata values for version {0}:", propertiesResponse.Value.VersionId);
            foreach (var item in propertiesResponse.Value.Metadata)
            {
                Console.WriteLine("Key:{0}  Value:{1}", item.Key, item.Value);
            }
        }
        else
        {
            Console.WriteLine("Version {0} has no metadata.", propertiesResponse.Value.VersionId);
        }
    }

   private static async Task ListBlobVersions(BlobContainerClient blobContainerClient, 
                                           int? segmentSize)
{
    try
    {
        // Call the listing operation, specifying that blob versions are returned.
        var resultSegment = blobContainerClient.GetBlobsAsync(default, BlobStates.Version)
            .AsPages(default, segmentSize);
//liping added 
        string blobName = "quickstart66ff3e47-7c98-436e-90cd-81734b087e6f.txt";
        BlockBlobClient blockBlobClient = blobContainerClient.GetBlockBlobClient(blobName);
        
        // Enumerate the blobs returned for each page.
        await foreach (Azure.Page<BlobItem> blobPage in resultSegment)
        {
            foreach (BlobItem blobItem in blobPage.Values)
            {
                string blobItemUri;

                // Check whether the blob item has a version ID.
                if (blobItem.VersionId != null)
                {
                    blobItemUri = string.Format("{0}/{1}?versionId={2}",
                        blobContainerClient.Uri,
                        blobItem.Name,
                        blobItem.VersionId);
                     BlockBlobClient newVersionBlob = blockBlobClient.WithVersion(blobItem.VersionId);
                     Response<BlobProperties> newPropertiesResponse = await newVersionBlob.GetPropertiesAsync();
                     PrintMetadata(newPropertiesResponse);
                     string localFilePath = Path.Combine("./data/", blobName); 
                     string versionString= blobItem.VersionId.Replace(":", "");

                     string downloadFilePath = localFilePath.Replace(".txt", versionString + ".txt");
                     
                     Console.WriteLine("\nDownloading blob to\n\t{0}\n", downloadFilePath);
                     BlobDownloadInfo download = await newVersionBlob.DownloadAsync();
                     using (FileStream downloadFileStream = File.OpenWrite(downloadFilePath))
                        {
                            await download.Content.CopyToAsync(downloadFileStream);
                            downloadFileStream.Close();
                        }
                    // Check whether the blob item is the latest version.
                    if ((bool)blobItem.IsLatestVersion.GetValueOrDefault())
                    {
                        blobItemUri += " (current version)";
                    }
                }
                else
                {
                    blobItemUri = string.Format("{0}/{1}",
                        blobContainerClient.Uri,
                        blobItem.Name);
                }
                Console.WriteLine(blobItemUri);
            }
            Console.WriteLine();
        }
    }
        catch (RequestFailedException e)
        {
            Console.WriteLine(e.Message);
            Console.ReadLine();
            throw;
        }
    }


    }
} 

