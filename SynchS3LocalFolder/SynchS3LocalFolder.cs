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
            Dictionary<string, string> FilesToCopy = new Dictionary<string, string>();
            AmazonS3Client client = new AmazonS3Client(Amazon.RegionEndpoint.USEast1);
            int iBackSlashIndex;
            string sLocalFilenameNoPath;
            string sS3FilenameNoPath;
            string sBucketName;
            string sBucketPrefix;
            string dirName = "";

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
                    if(args[0].Substring(0,5)=="s3://"||args[0].Substring(0,5)=="S3://")
                    {
                        iBackSlashIndex = args[0].IndexOf('/',5);
                        sBucketName = args[0].Substring(5, iBackSlashIndex - 5);
                        sBucketPrefix = args[0].Substring(iBackSlashIndex + 1, args[0].Length - iBackSlashIndex - 1);

                        dirName = args[1];
                    }
                    else
                    {
                        dirName = args[0];

                        iBackSlashIndex = args[1].IndexOf('/', 5);
                        sBucketName = args[1].Substring(5, iBackSlashIndex - 5);
                        sBucketPrefix = args[1].Substring(iBackSlashIndex + 1, args[1].Length - iBackSlashIndex - 1);
                    }

                    // ---------------------------------------------- Local Files ----------------------------------------------
                    string[] fileEntries = Directory.GetFiles(dirName);
                    foreach (string sCurrFileName in fileEntries)
                    {
                        iBackSlashIndex = sCurrFileName.LastIndexOf('\\') + 1;
                        sLocalFilenameNoPath = sCurrFileName.Substring(iBackSlashIndex, sCurrFileName.Length - iBackSlashIndex);
                        LocalFiles.Add(sLocalFilenameNoPath, sLocalFilenameNoPath);
                    }
                    // ---------------------------------------------------------------------------------------------------------

                    // ---------------------------------------------- S3 Files ----------------------------------------------
                    ListObjectsRequest request = new ListObjectsRequest();
                    request.BucketName = sBucketName;
                    request.Prefix = sBucketPrefix;

                    ListObjectsResponse response = client.ListObjects(request);

                    foreach (S3Object entry in response.S3Objects)
                    {
                        sS3FilenameNoPath = entry.Key.Substring(request.Prefix.Length + 1, entry.Key.Length - request.Prefix.Length - 1);
                        if(sS3FilenameNoPath.Length > 0)
                            S3Files.Add(sS3FilenameNoPath, sS3FilenameNoPath);
                    }
                    // ------------------------------------------------------------------------------------------------------
                }
                catch (Exception e)
                {
                    Console.WriteLine("Erro! " + e.Message);
                    return;
                }
            }
        }
    }
}
