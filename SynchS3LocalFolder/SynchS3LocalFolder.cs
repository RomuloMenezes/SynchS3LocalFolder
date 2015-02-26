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
            AmazonS3Client client = new AmazonS3Client(Amazon.RegionEndpoint.USEast1);

            // ---------------------------------------------- Local Files ----------------------------------------------
            string dirName = "D:\\openPDC\\Archive\\";

            string[] fileEntries = Directory.GetFiles(dirName);
            foreach (string sCurrFileName in fileEntries)
            {
                LocalFiles.Add(sCurrFileName, sCurrFileName);
            }
            // ---------------------------------------------------------------------------------------------------------

            // ---------------------------------------------- S3 Files ----------------------------------------------
            ListObjectsRequest request = new ListObjectsRequest();
            request.BucketName = "pmu-data";
            request.Prefix = "Archive";

            ListObjectsResponse response = client.ListObjects(request);

            foreach (S3Object entry in response.S3Objects)
            {
                S3Files.Add(entry.Key, entry.Key);
            }
            // ------------------------------------------------------------------------------------------------------
        }
    }
}
