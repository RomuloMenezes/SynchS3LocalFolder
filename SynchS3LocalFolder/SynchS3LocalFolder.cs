using System;
using System.Collections;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.ServiceProcess;
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
            Dictionary<string, string> z_Files = new Dictionary<string, string>();
            AmazonS3Client client = new AmazonS3Client(Amazon.RegionEndpoint.USEast1);
            TransferUtility transfer = new TransferUtility(client);
            int iBackSlashIndex;
            string sSourceBucketName = "";
            string sSourceBucketPrefix = "";
            string sTargetBucketName = "";
            string sTargetBucketPrefix = "";
            string dirName = "";
            bool bLocalFilesPresent = false;
            CopyObjectRequest copyRequest = new CopyObjectRequest();
            CopyObjectResponse copyResponse;
            DeleteObjectRequest deleteRequest = new DeleteObjectRequest();
            bool synchToTarget = false;
            bool SynchToSource = false;
            bool loadNew = false;
            bool loadAll = false;
            string sLatestFile = "";
            int waitForToken = 0;
            string appFolder = AppDomain.CurrentDomain.BaseDirectory;
            string sServiceStatus = "";
            string sParams = "";
            AppendOnFile LogWriter = new AppendOnFile();
            StringBuilder sTextToAppend = new StringBuilder();

            // StreamWriter LogWriter = new StreamWriter(appFolder + "\\PMU.log", true); // Open appending
            // LogWriter.WriteLine(DateTime.Now.ToString("yyyyMMdd HH:mm:ss") + " - >>> DEBUG <<< Begin execution >>>");
            LogWriter.file = appFolder + "\\PMU.log";
            sTextToAppend.Append(DateTime.Now.ToString("yyyyMMdd HH:mm:ss") + " - >>> DEBUG <<< Begin execution >>>");
            LogWriter.sTextToAppend = sTextToAppend;
            LogWriter.Append();
            sTextToAppend.Clear();

            // Recovering last file saved
            StreamReader MyReader = new StreamReader(appFolder + "LastFile.pmu");
            string lastFileSavedFromFile = MyReader.ReadLine();
            MyReader.Close();

            // LogWriter.WriteLine(DateTime.Now.ToString("yyyyMMdd HH:mm:ss") + " - Last file read: " + lastFileSavedFromFile);
            LogWriter.file = appFolder + "\\PMU.log";
            sTextToAppend.Append(DateTime.Now.ToString("yyyyMMdd HH:mm:ss") + " - Last file read: " + lastFileSavedFromFile);
            LogWriter.sTextToAppend = sTextToAppend;
            LogWriter.Append();
            sTextToAppend.Clear();

            // Console.WriteLine("LastFileSaved read from App.Config: " + lastFileSavedFromFile);
            
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

            if (args.Length > 1)
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

                    sParams = args[0];
                    // ---------------------------------------------------------------------------------------------------------------------------
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            
                    // ---------------------------------------------- Target location (parameter 2) ----------------------------------------------
                    if (args[1].Substring(0, 5) == "s3://" || args[1].Substring(0, 5) == "S3://")
                    {
                        iBackSlashIndex = args[1].IndexOf('/', 5);
                        sTargetBucketName = args[1].Substring(5, iBackSlashIndex - 5);
                        sTargetBucketPrefix = args[1].Substring(iBackSlashIndex + 1, args[1].Length - iBackSlashIndex - 1);

                        // ---------- Checking if target bucket exists. If it doesn't, create it ----------
                        GetObjectRequest checkBucketRequest = new GetObjectRequest();
                        checkBucketRequest.BucketName = sTargetBucketName + "/" + sTargetBucketPrefix;
                        try
                        {
                            GetObjectResponse response = client.GetObject(checkBucketRequest);
                        }
                        catch // Target bucket does not exist. The code within the following catch creates it.
                        {
                            PutBucketRequest createBucketRequest = new PutBucketRequest();
                            createBucketRequest.BucketName = sTargetBucketName + "/" + sTargetBucketPrefix + "/";
                            client.PutBucket(createBucketRequest);
                        }
                        // --------------------------------------------------------------------------------

                        FilesOnTarget = GetS3Files(client, sTargetBucketName, sTargetBucketPrefix);
                    }
                    else
                    {
                        dirName = args[1];

                        // ---------- Checking if target folder exists. If it doesn't, create it ----------
                        if (!Directory.Exists(dirName))
                            Directory.CreateDirectory(dirName);
                        // --------------------------------------------------------------------------------

                        FilesOnTarget = GetLocalFiles(dirName);
                        bLocalFilesPresent = true;
                    }

                    sParams += " " + args[1];
                    // ---------------------------------------------------------------------------------------------------------------------------

                    // LogWriter.WriteLine(DateTime.Now.ToString("yyyyMMdd HH:mm:ss") + " - Parameters 1 and 2 read.");
                    LogWriter.file = appFolder + "\\PMU.log";
                    sTextToAppend.Append(DateTime.Now.ToString("yyyyMMdd HH:mm:ss") + " - Parameters 1 and 2 read.");
                    LogWriter.sTextToAppend = sTextToAppend;
                    LogWriter.Append();
                    sTextToAppend.Clear();

                    // --------------------------------------------------- Parameters 3 and 4 ----------------------------------------------------
                    // Parameters 3 and 4 are optional and the order in which they are passed is irrelevant. They are flags that determine:
                    //
                    //     1. if a copy action should be synched between a producer execution and a consumer one. In that case, after all
                    //        the files have been copied from source to target, a synch file (SynchToken.pmu) is also moved. The consumer
                    //        execution only starts when the synch file is found. The consumer execution deletes the synch file when the
                    //        copying is done.
                    //        For this synching to happen the producer execution must be called with the parameter "SynchToTarget" and the 
                    //        consumer execution must be called with the parameter "SynchToSource"
                    //
                    //     2. the code only copies the files that are in the source and aren't in the target. The parameter "All" or "New"
                    //        determines whether all the missing files are copied - in case the parameter "All" is passed - or only those
                    //        who are later than the last file copied in a previous execution - in case the parameter "New" is passed.
                    //        With that purpose, in each execution the name of the later file copied is saved in the app.config.

                    if (args.Length == 3)
                    {
                        switch(args[2])
                        {
                            case "SynchToTarget":
                                synchToTarget = true;
                                break;
                            case "SynchToSource":
                                SynchToSource = true;
                                break;
                            case "New":
                                loadNew = true;
                                break;
                            case "All":
                                loadAll = true;
                                break;
                        }
                        sParams += " " + args[2];
                    }
                    else
                    {
                        if (args.Length == 4)
                        {
                            switch (args[2])
                            {
                                case "SynchToTarget":
                                    synchToTarget = true;
                                    break;
                                case "SynchToSource":
                                    SynchToSource = true;
                                    break;
                                case "New":
                                    loadNew = true;
                                    break;
                                case "All":
                                    loadAll = true;
                                    break;
                            }
                            switch (args[3])
                            {
                                case "SynchToTarget":
                                    if(!SynchToSource)
                                        synchToTarget = true;
                                    break;
                                case "SynchToSource":
                                    if (!synchToTarget)
                                        SynchToSource = true;
                                    break;
                                case "New":
                                    if (!loadAll)
                                        loadNew = true;
                                    break;
                                case "All":
                                    if (!loadNew)
                                        loadAll = true;
                                    break;
                            }
                        }
                        sParams += " " + args[2] + " " + args[3];
                    }

                    // LogWriter.WriteLine(DateTime.Now.ToString("yyyyMMdd HH:mm:ss") + " - Parameters 3 and 4 treated.");
                    LogWriter.file = appFolder + "\\PMU.log";
                    sTextToAppend.Append(DateTime.Now.ToString("yyyyMMdd HH:mm:ss") + " - Parameters 3 and 4 treated.");
                    LogWriter.sTextToAppend = sTextToAppend;
                    LogWriter.Append();
                    sTextToAppend.Clear();

                    if (!(loadAll || loadNew)) // Load all is default
                        loadAll = true;

                    // ---------------------------------------------------------------------------------------------------------------------------

                    // LogWriter.WriteLine(DateTime.Now.ToString("yyyyMMdd HH:mm:ss") + " - Begin generation.");
                    // LogWriter.WriteLine(DateTime.Now.ToString("yyyyMMdd HH:mm:ss") + " - Parameters: " + sParams);
                    LogWriter.file = appFolder + "\\PMU.log";
                    sTextToAppend.Append(DateTime.Now.ToString("yyyyMMdd HH:mm:ss") + " - Begin generation." + Environment.NewLine + 
                                         DateTime.Now.ToString("yyyyMMdd HH:mm:ss") + " - Parameters: " + sParams);
                    LogWriter.sTextToAppend = sTextToAppend;
                    LogWriter.Append();
                    sTextToAppend.Clear();

                    if(SynchToSource)
                    {
                        while(!FilesOnSource.ContainsKey("SynchToken.pmu") && waitForToken < 6)
                        {
                            // LogWriter.WriteLine(DateTime.Now.ToString("yyyyMMdd HH:mm:ss") + " - Waiting for token. Attempt " + Convert.ToString(waitForToken + 1));
                            LogWriter.file = appFolder + "\\PMU.log";
                            sTextToAppend.Append(DateTime.Now.ToString("yyyyMMdd HH:mm:ss") + " - Waiting for token. Attempt " + Convert.ToString(waitForToken + 1));
                            LogWriter.sTextToAppend = sTextToAppend;
                            LogWriter.Append();
                            sTextToAppend.Clear();

                            System.Threading.Thread.Sleep(300000); // Wait for 5 minutes
                            waitForToken++;
                            FilesOnSource = GetS3Files(client, sSourceBucketName, sSourceBucketPrefix);
                        }
                        if(waitForToken == 6)
                        {
                            // LogWriter.WriteLine(DateTime.Now.ToString("yyyyMMdd HH:mm:ss") + " - TIMEOUT!!! EXECUTION ABORTED.");
                            // LogWriter.WriteLine(DateTime.Now.ToString("yyyyMMdd HH:mm:ss") + " -------------------------------------");
                            // LogWriter.Close();
                            LogWriter.file = appFolder + "\\PMU.log";
                            sTextToAppend.Append(DateTime.Now.ToString("yyyyMMdd HH:mm:ss") + " - TIMEOUT!!! EXECUTION ABORTED." + Environment.NewLine +
                                                 DateTime.Now.ToString("yyyyMMdd HH:mm:ss") + " -------------------------------------");
                            LogWriter.sTextToAppend = sTextToAppend;
                            LogWriter.Append();
                            sTextToAppend.Clear();

                            return;
                        }
                    }

                    sLatestFile = lastFileSavedFromFile;
                    foreach(string currFile in FilesOnSource.Keys)
                    {
                        if (!FilesOnTarget.ContainsKey(currFile))
                        {
                            // Copying the file ppa_archive.d may cause problems to the openPDC process, because the AWS API locks the file while it's being copied
                            // and the openPDC process may try to rollover in the meantime. So, a copy of the file is made, and the copy is treated.
                            // The ppa_archive.d, along with the .dat files, is necessary for reading historic files.

                            if (currFile == "ppa_archive.d")
                            {
                                if (args[0].Substring(0, 5) == "s3://" || args[0].Substring(0, 5) == "S3://") // Local folder is the source
                                    FilesToCopy.Add(currFile, currFile);
                                else
                                {
                                    if (File.Exists(dirName + "\\z_" + currFile))
                                        File.Delete(dirName + "\\z_" + currFile);
                                    File.Copy(dirName + "\\" + currFile, dirName + "\\z_" + currFile); // Use "prefix" z_ so that this file name is greater than lastFileSavedFromFile
                                    FilesToCopy.Add("z_" + currFile, "z_" + currFile);
                                }
                            }
                            else
                            {
                                FilesToCopy.Add(currFile, currFile);
                                if (!currFile.StartsWith("z_"))
                                    if (String.Compare(currFile, sLatestFile, true) > 0 && currFile.Substring(currFile.Length - 2, 2) == ".d")
                                        sLatestFile = currFile;
                            }
                        }
                        else
                        {
                            // The .dat files, along with the ppa_archive.d file, are necessary for reading historic files. Since in most executions these files are
                            // already in the terget folder - and therefore, due to the if above, are not added to FilesToCopy - their copy is forced in the following
                            // piece of code. Like the ppa_archive.d, they have to be copied in order to be treated.
                            if ((currFile.EndsWith(".dat") && !currFile.StartsWith("z_") || currFile == "ppa_archive.d"))
                            {
                                if (args[0].Substring(0, 5) == "s3://" || args[0].Substring(0, 5) == "S3://") // S3 is the source
                                    FilesToCopy.Add(currFile, currFile);
                                else
                                {
                                    if (File.Exists(dirName + "\\z_" + currFile))
                                        File.Delete(dirName + "\\z_" + currFile);
                                    File.Copy(dirName + "\\" + currFile, dirName + "\\z_" + currFile); // Use "prefix" z_ so that this file name is greater than lastFileSavedFromFile
                                    FilesToCopy.Add("z_" + currFile, "z_" + currFile);
                                }
                            }
                            //else
                            //{
                            //    if (currFile == "ppa_archive.d")
                            //    {
                            //        if (args[0].Substring(0, 5) == "s3://" || args[0].Substring(0, 5) == "S3://") // Local folder is the source
                            //            FilesToCopy.Add(currFile, currFile);
                            //        else
                            //        {
                            //            if (File.Exists(dirName + "\\z_" + currFile))
                            //                File.Delete(dirName + "\\z_" + currFile);
                            //            File.Copy(dirName + "\\" + currFile, dirName + "\\z_" + currFile); // Use "prefix" z_ so that this file name is greater than lastFileSavedFromFile
                            //            FilesToCopy.Add("z_" + currFile, "z_" + currFile);
                            //        }                                      
                            //    }
                            //}
                        }
                    }

                    foreach(string currFile in FilesToCopy.Keys)
                    {
                        if (bLocalFilesPresent)
                        {
                            if(loadAll)
                            {
                                try
                                {
                                    if (args[0].Substring(0, 5) == "s3://" || args[0].Substring(0, 5) == "S3://") // S3 is the source
                                        transfer.Download(dirName + "\\" + currFile, sSourceBucketName + "/" + sSourceBucketPrefix, currFile);
                                    else // Local folder is the source
                                        transfer.Upload(dirName + "/" + currFile, sTargetBucketName + "/" + sTargetBucketPrefix, currFile);
                                }
                                catch (Exception e)
                                {
                                    sServiceStatus = GetServiceStatus("openPDC");
                                    LogWriter.file = appFolder + "\\PMU.log";
                                    sTextToAppend.Append(DateTime.Now.ToString("yyyyMMdd HH:mm:ss") + " - File " + currFile + " NOT COPPIED!!! " + e.Message + Environment.NewLine +
                                                         DateTime.Now.ToString("yyyyMMdd HH:mm:ss") + " - Service was " + sServiceStatus);
                                    LogWriter.sTextToAppend = sTextToAppend;
                                    LogWriter.Append();
                                    sTextToAppend.Clear();
                                    // LogWriter.WriteLine(DateTime.Now.ToString("yyyyMMdd HH:mm:ss") + " - File " + currFile + " NOT COPPIED!!! " + e.Message);
                                    // sServiceStatus = GetServiceStatus("openPDC");
                                    // LogWriter.WriteLine(DateTime.Now.ToString("yyyyMMdd HH:mm:ss") + " - Service was " + sServiceStatus);
                                }
                            }
                            else
                            {
                                try
                                {
                                    if (loadNew && (String.Compare(currFile, lastFileSavedFromFile) > 0 || currFile == "ppa_archive.d"))
                                    {
                                        if (args[0].Substring(0, 5) == "s3://" || args[0].Substring(0, 5) == "S3://") // S3 is the source
                                            transfer.Download(dirName + "\\" + currFile, sSourceBucketName + "/" + sSourceBucketPrefix, currFile);
                                        else // Local folder is the source
                                        {
                                            transfer.Upload(dirName + "/" + currFile, sTargetBucketName + "/" + sTargetBucketPrefix, currFile);
                                        }
                                    }
                                }
                                catch (Exception e)
                                {
                                    // LogWriter.WriteLine(DateTime.Now.ToString("yyyyMMdd HH:mm:ss") + " - File " + currFile + " NOT COPPIED!!! " + e.Message);
                                    // sServiceStatus = GetServiceStatus("openPDC");
                                    // LogWriter.WriteLine(DateTime.Now.ToString("yyyyMMdd HH:mm:ss") + " - Service was " + sServiceStatus);

                                    sServiceStatus = GetServiceStatus("openPDC");
                                    LogWriter.file = appFolder + "\\PMU.log";
                                    sTextToAppend.Append(DateTime.Now.ToString("yyyyMMdd HH:mm:ss") + " - File " + currFile + " NOT COPPIED!!! " + e.Message + Environment.NewLine +
                                                         DateTime.Now.ToString("yyyyMMdd HH:mm:ss") + " - Service was " + sServiceStatus);
                                    LogWriter.sTextToAppend = sTextToAppend;
                                    LogWriter.Append();
                                    sTextToAppend.Clear();
                                }
                            }
                        }
                        else
                        {
                            try
                            {
                                if (loadAll)
                                {
                                    copyRequest.SourceBucket = sSourceBucketName + "/" + sSourceBucketPrefix;
                                    copyRequest.SourceKey = currFile;
                                    copyRequest.DestinationBucket = sTargetBucketName + "/" + sTargetBucketPrefix;
                                    copyRequest.DestinationKey = currFile;
                                    copyResponse = client.CopyObject(copyRequest);
                                }
                                else
                                {
                                    if (loadNew && (String.Compare(currFile, lastFileSavedFromFile) > 0 || currFile == "ppa_archive.d"))
                                    {
                                        copyRequest.SourceBucket = sSourceBucketName + "/" + sSourceBucketPrefix;
                                        copyRequest.SourceKey = currFile;
                                        copyRequest.DestinationBucket = sTargetBucketName + "/" + sTargetBucketPrefix;
                                        copyRequest.DestinationKey = currFile;
                                        copyResponse = client.CopyObject(copyRequest);
                                    }
                                }
                            }
                            catch (Exception e)
                            {
                                // LogWriter.WriteLine(DateTime.Now.ToString("yyyyMMdd HH:mm:ss") + " - File " + currFile + " NOT COPPIED!!! " + e.Message);
                                // sServiceStatus = GetServiceStatus("openPDC");
                                // LogWriter.WriteLine(DateTime.Now.ToString("yyyyMMdd HH:mm:ss") + " - Service was " + sServiceStatus);

                                sServiceStatus = GetServiceStatus("openPDC");
                                LogWriter.file = appFolder + "\\PMU.log";
                                sTextToAppend.Append(DateTime.Now.ToString("yyyyMMdd HH:mm:ss") + " - File " + currFile + " NOT COPPIED!!! " + e.Message + Environment.NewLine +
                                                     DateTime.Now.ToString("yyyyMMdd HH:mm:ss") + " - Service was " + sServiceStatus);
                                LogWriter.sTextToAppend = sTextToAppend;
                                LogWriter.Append();
                                sTextToAppend.Clear();
                            }
                        }
                        Console.WriteLine(currFile + " treated.");
                    }

                    if(synchToTarget)
                    {
                        if (bLocalFilesPresent)
                        {                                                                                                                                                                                                                                                                                                                                                                                                                                                   
                            if (args[0].Substring(0, 5) == "s3://" || args[0].Substring(0, 5) == "S3://") // S3 is the source
                                transfer.Download("SynchToken.pmu", sSourceBucketName + "/" + sSourceBucketPrefix, "SynchToken.pmu");
                            else // Local folder is the source
                            {
                                transfer.Upload("SynchToken.pmu", sTargetBucketName + "/" + sTargetBucketPrefix, "SynchToken.pmu");
                            }
                        }
                        else
                        {
                            copyRequest.SourceBucket = sSourceBucketName + "/" + sSourceBucketPrefix;
                            copyRequest.SourceKey = "SynchToken.pmu";
                            copyRequest.DestinationBucket = sTargetBucketName + "/" + sTargetBucketPrefix;
                            copyRequest.DestinationKey = "SynchToken.pmu";
                            copyResponse = client.CopyObject(copyRequest);
                        }
                    }

                    if (SynchToSource)
                    {
                        if (args[0].Substring(0, 5) == "s3://" || args[0].Substring(0, 5) == "S3://") // S3 is the source
                        {
                            deleteRequest.BucketName = sSourceBucketName + "/" + sSourceBucketPrefix;
                            deleteRequest.Key = "SynchToken.pmu";
                            client.DeleteObject(deleteRequest);
                        }
                        else // Local folder is the source
                        {
                            System.IO.File.Delete(dirName + "\\SynchToken.pmu");
                        }
                    }

                    // Deleting the z_ files on local storage and renaming them on S3
                    if (!(args[0].Substring(0, 5) == "s3://" || args[0].Substring(0, 5) == "S3://")) // Local storage is the source
                    {
                        z_Files = GetLocalFiles(dirName, "z_*");
                        foreach(string z_File in z_Files.Keys)
                        {
                            try
                            {
                                copyRequest.SourceBucket = sTargetBucketName + "/" + sTargetBucketPrefix;
                                copyRequest.SourceKey = z_File;
                                copyRequest.DestinationBucket = sTargetBucketName + "/" + sTargetBucketPrefix;
                                copyRequest.DestinationKey = z_File.Substring(2);
                                copyResponse = client.CopyObject(copyRequest);

                                // LogWriter.WriteLine(DateTime.Now.ToString("yyyyMMdd HH:mm:ss") + " - " + z_File + " renamed on S3");
                                LogWriter.file = appFolder + "\\PMU.log";
                                sTextToAppend.Append(DateTime.Now.ToString("yyyyMMdd HH:mm:ss") + " - " + z_File + " renamed on S3");
                                LogWriter.sTextToAppend = sTextToAppend;
                                LogWriter.Append();
                                sTextToAppend.Clear();

                                deleteRequest.BucketName = sTargetBucketName + "/" + sTargetBucketPrefix;
                                deleteRequest.Key = z_File;
                                client.DeleteObject(deleteRequest);
                                
                                // LogWriter.WriteLine(DateTime.Now.ToString("yyyyMMdd HH:mm:ss") + " - " + z_File + " deleted from S3");
                                LogWriter.file = appFolder + "\\PMU.log";
                                sTextToAppend.Append(DateTime.Now.ToString("yyyyMMdd HH:mm:ss") + " - " + z_File + " deleted from S3");
                                LogWriter.sTextToAppend = sTextToAppend;
                                LogWriter.Append();
                                sTextToAppend.Clear();
                            }
                            catch (Exception e)
                            {
                                // LogWriter.WriteLine(DateTime.Now.ToString("yyyyMMdd HH:mm:ss") + " - Error treating file " + z_File + " on S3");
                                // LogWriter.WriteLine(DateTime.Now.ToString("yyyyMMdd HH:mm:ss") + " - Error Message: " + e.Message);
                                LogWriter.file = appFolder + "\\PMU.log";
                                sTextToAppend.Append(DateTime.Now.ToString("yyyyMMdd HH:mm:ss") + " - Error treating file " + z_File + " on S3" + Environment.NewLine +
                                                     DateTime.Now.ToString("yyyyMMdd HH:mm:ss") + " - Error Message: " + e.Message);
                                LogWriter.sTextToAppend = sTextToAppend;
                                LogWriter.Append();
                                sTextToAppend.Clear();
                            }
                            
                            try
                            {
                                System.IO.File.Delete(dirName + "\\" + z_File);
                                // LogWriter.WriteLine(DateTime.Now.ToString("yyyyMMdd HH:mm:ss") + " - " + z_File + " deleted from local folder");
                                LogWriter.file = appFolder + "\\PMU.log";
                                sTextToAppend.Append(DateTime.Now.ToString("yyyyMMdd HH:mm:ss") + " - " + z_File + " deleted from local folder");
                                LogWriter.sTextToAppend = sTextToAppend;
                                LogWriter.Append();
                                sTextToAppend.Clear();
                            }
                            catch (Exception e)
                            {
                                // LogWriter.WriteLine(DateTime.Now.ToString("yyyyMMdd HH:mm:ss") + " - Error treating local file " + z_File);
                                // LogWriter.WriteLine(DateTime.Now.ToString("yyyyMMdd HH:mm:ss") + " - Error Message: " + e.Message);
                                LogWriter.file = appFolder + "\\PMU.log";
                                sTextToAppend.Append(DateTime.Now.ToString("yyyyMMdd HH:mm:ss") + " - Error treating local file " + z_File + Environment.NewLine +
                                                     DateTime.Now.ToString("yyyyMMdd HH:mm:ss") + " - Error Message: " + e.Message);
                                LogWriter.sTextToAppend = sTextToAppend;
                                LogWriter.Append();
                                sTextToAppend.Clear();
                            }
                        }
                    }

                    // Storing last file saved
                    System.IO.File.Delete(appFolder + "LastFile.pmu");
                    StreamWriter MyWriter = new StreamWriter(appFolder + "LastFile.pmu");
                    MyWriter.Write(sLatestFile);
                    MyWriter.Close();

                    // LogWriter.WriteLine(DateTime.Now.ToString("yyyyMMdd HH:mm:ss") + " - End of generation.");
                    // LogWriter.WriteLine(DateTime.Now.ToString("yyyyMMdd HH:mm:ss") + " -------------------------------------");
                    // LogWriter.Close();
                    LogWriter.file = appFolder + "\\PMU.log";
                    sTextToAppend.Append(DateTime.Now.ToString("yyyyMMdd HH:mm:ss") + " - End of generation." + Environment.NewLine +
                                         DateTime.Now.ToString("yyyyMMdd HH:mm:ss") + " -------------------------------------");
                    LogWriter.sTextToAppend = sTextToAppend;
                    LogWriter.Append();
                    sTextToAppend.Clear();

                } // end try
                catch (Exception e)
                {
                    Console.WriteLine(DateTime.Now.ToString("yyyyMMdd HH:mm:ss") + "Erro! " + e.Message);
                    LogWriter.file = appFolder + "\\PMU.log";
                    sTextToAppend.Append(DateTime.Now.ToString("yyyyMMdd HH:mm:ss") + "Erro! " + e.Message);
                    LogWriter.sTextToAppend = sTextToAppend;
                    LogWriter.Append();
                    sTextToAppend.Clear();
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

        private static Dictionary<string, string> GetLocalFiles(string dirName, string wildcard)
        {
            int iBackSlashIndex;
            string sLocalFilenameNoPath;
            Dictionary<string, string> LocalFiles = new Dictionary<string, string>();
            string[] fileEntries = Directory.GetFiles(dirName, wildcard);
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

        private static string GetServiceStatus(string serviceName)
        {
            ServiceController sc = new ServiceController(serviceName);

            switch (sc.Status)
            {
                case ServiceControllerStatus.Running:
                    return "Running";
                case ServiceControllerStatus.Stopped:
                    return "Stopped";
                case ServiceControllerStatus.Paused:
                    return "Paused";
                case ServiceControllerStatus.StopPending:
                    return "Stopping";
                case ServiceControllerStatus.StartPending:
                    return "Starting";
                default:
                    return "Status Changing";
            }
        }

        private class AppendOnFile
        {
            public string file;
            public StringBuilder sTextToAppend = new StringBuilder();

            public void Append()
            {
                StreamWriter localOutputFile;
                localOutputFile = new System.IO.StreamWriter(file, true);
                localOutputFile.WriteLine(sTextToAppend);
                localOutputFile.Close();
            }
        }
    }
}
