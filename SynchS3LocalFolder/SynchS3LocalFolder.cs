using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using Amazon;
using Amazon.S3;
using Amazon.S3.IO;
using Amazon.S3.Model;
using Amazon.S3.Transfer;

namespace SynchS3LocalFolder
{
    class SynchS3LocalFolder
    {
        static void Main(string[] args)
        {
            Dictionary<string, string> LocalFiles = new Dictionary<string, string>();
            Dictionary<string, string> S3Files = new Dictionary<string, string>();
            Dictionary<string, string> FilesOnSource = new Dictionary<string, string>();
            Dictionary<string, string> FilesOnTarget = new Dictionary<string, string>();
            Dictionary<string, string> FilesToCopy = new Dictionary<string, string>();
            AmazonS3Client client = new AmazonS3Client(Amazon.RegionEndpoint.USEast1);
            TransferUtility transfer = new TransferUtility(client);
            int iBackSlashIndex;
            string sSourceBucketName = "";
            string sSourceBucketPrefix = "";
            string sTargetBucketName = "";
            string sTargetBucketPrefix = "";
            string dirName = "";
            bool bLocalFilesPresent = false;
            CopyObjectRequest request = new CopyObjectRequest();
            CopyObjectResponse response;
            
            if (args.Length == 0)
            {
                Console.WriteLine("Please select source and target folders");
                return;
            }

            if (args.Length == 1)
            {
                Console.WriteLine("Please select target folders");
                return;
            }

            if (args.Length == 2)
            {
                try
                {
                    // ---------------------------------------------- Source location (parameter 1) ----------------------------------------------
                    if(args[0].Substring(0,5)=="s3://"||args[0].Substring(0,5)=="S3://")
                    {
                        iBackSlashIndex = args[0].IndexOf('/',5);
                        sSourceBucketName = args[0].Substring(5, iBackSlashIndex - 5);
                        sSourceBucketPrefix = args[0].Substring(iBackSlashIndex + 1, args[0].Length - iBackSlashIndex - 1);
                        FilesOnSource = GetS3Files(client, sSourceBucketName, sSourceBucketPrefix);
                    }
                    else
                    {
                        dirName = args[0];
                        FilesOnSource = GetLocalFiles(dirName);
                        bLocalFilesPresent = true;
                    }
                    // ---------------------------------------------------------------------------------------------------------------------------

                    // ---------------------------------------------- Target location (parameter 2) ----------------------------------------------
                    if (args[1].Substring(0, 5) == "s3://" || args[1].Substring(0, 5) == "S3://")
                    {
                        iBackSlashIndex = args[1].IndexOf('/', 5);
                        sTargetBucketName = args[1].Substring(5, iBackSlashIndex - 5);
                        sTargetBucketPrefix = args[1].Substring(iBackSlashIndex + 1, args[1].Length - iBackSlashIndex - 1);
                        FilesOnTarget = GetS3Files(client, sTargetBucketName, sTargetBucketPrefix);
                    }
                    else
                    {
                        dirName = args[1];
                        FilesOnTarget = GetLocalFiles(dirName);
                        bLocalFilesPresent = true;
                    }
                    // ---------------------------------------------------------------------------------------------------------------------------

                    foreach(string currFile in FilesOnSource.Keys)
                    {
                        if (!FilesOnTarget.ContainsKey(currFile))
                            FilesToCopy.Add(currFile, currFile);
                    }

                    foreach(string currFile in FilesToCopy.Keys)
                    {
                        if (bLocalFilesPresent)
                        {
                            if (args[0].Substring(0, 5) == "s3://" || args[0].Substring(0, 5) == "S3://") // S3 is the source
                                transfer.Download(dirName + "\\" + currFile, sSourceBucketName + "/" + sSourceBucketPrefix, currFile);
                            else // Local folder is the source
                                transfer.Upload(dirName + "/" + currFile, sTargetBucketName + "/" + sTargetBucketPrefix, currFile);
                        }
                        else
                        {
                            request.SourceBucket = sSourceBucketName + "/" + sSourceBucketPrefix;
                            request.SourceKey= currFile;
                            request.DestinationBucket = sTargetBucketName + "/" + sTargetBucketPrefix;
                            request.DestinationKey = currFile;
                            response = client.CopyObject(request);
                        }
                    }

                } // end try
                catch (Exception e)
                {
                    Console.WriteLine("Erro! " + e.Message);
                    return;
                }
            }
        }

        private static Dictionary<string, string> GetLocalFiles(string dirName)
        {
            int iBackSlashIndex;
            string sLocalFilenameNoPath;
            Dictionary<string, string> LocalFiles = new Dictionary<string, string>();
            string[] fileEntries = Directory.GetFiles(dirName);
            foreach (string sCurrFileName in fileEntries)
            {
                iBackSlashIndex = sCurrFileName.LastIndexOf('\\') + 1;
                sLocalFilenameNoPath = sCurrFileName.Substring(iBackSlashIndex, sCurrFileName.Length - iBackSlashIndex);
                LocalFiles.Add(sLocalFilenameNoPath, sLocalFilenameNoPath);
            }
            return LocalFiles;
        }

        private static Dictionary<string, string> GetS3Files(AmazonS3Client client, string sBucketName, string sBucketPrefix)
        {
            string sS3FilenameNoPath;
            Dictionary<string, string> S3Files = new Dictionary<string, string>();
            ListObjectsRequest request = new ListObjectsRequest();
            request.BucketName = sBucketName;
            request.Prefix = sBucketPrefix;
            ListObjectsResponse response = new ListObjectsResponse();
            request.MaxKeys = 50000;

            do
            {
                response = client.ListObjects(request);

                foreach (S3Object entry in response.S3Objects)
                {
                    sS3FilenameNoPath = entry.Key.Substring(request.Prefix.Length + 1, entry.Key.Length - request.Prefix.Length - 1);
                    if (sS3FilenameNoPath.Length > 0)
                        S3Files.Add(sS3FilenameNoPath, sS3FilenameNoPath);
                }

                // REMEMBER ListObjects returns not more than 1000 objects. If response is truncated, set the marker to get the next 
                // set of keys.
                if (response.IsTruncated)
                {
                    request.Marker = response.NextMarker;
                }
                else
                {
                    request = null;
                }

            } while (request != null);
            return S3Files;
        }
    }
}
