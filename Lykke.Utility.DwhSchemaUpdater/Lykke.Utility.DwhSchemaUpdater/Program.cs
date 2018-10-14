using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.RetryPolicies;

namespace Lykke.Utility.DwhSchemaUpdater
{
    class Program
    {
        private const string _blobAccountNameArg = "--blobAccountName";
        private const string _blobAccountKeyArg = "--blobAccountKey";
        private const string _sqlCredentialsArg = "--sqlCredentials";
        private const string _containerArg = "--blobContainer";
        private const string _structureFileName = "TableStructure.str2";
        private const string _structureUpdateFileName = "lastStructureUpdate.txt";
        private const int _maxRetryCount = 5;

        private static readonly BlobRequestOptions _blobRequestOptions = new BlobRequestOptions
        {
            RetryPolicy = new ExponentialRetry(TimeSpan.FromSeconds(5), 5),
            MaximumExecutionTime = TimeSpan.FromMinutes(60),
            ServerTimeout = TimeSpan.FromMinutes(60)
        };

        static void Main(string[] args)
        {
            try
            {
                var argsList = args.ToList();
                int accountNameIndex = argsList.IndexOf(_blobAccountNameArg);
                int accountKeyIndex = argsList.IndexOf(_blobAccountKeyArg);
                int sqlCredsIndex = argsList.IndexOf(_sqlCredentialsArg);
                if (accountNameIndex == -1 || accountNameIndex + 1 >= args.Length
                                           || accountKeyIndex == -1 || accountKeyIndex + 1 >= args.Length
                                           || sqlCredsIndex == -1 || sqlCredsIndex + 1 >= args.Length)
                {
                    Console.WriteLine(
                        $"Usage: {_blobAccountNameArg} %account_name% {_blobAccountKeyArg} %account_key% {_sqlCredentialsArg} %sql_cedentials% [{_containerArg} %blob_container%]");
                }
                else
                {
                    string accountName = args[accountNameIndex + 1];
                    string accountKey = args[accountKeyIndex + 1];
                    string sqlConnString = args[sqlCredsIndex + 1];

                    var containerIndex = argsList.IndexOf(_containerArg);
                    string containerName = containerIndex != -1 && containerIndex + 1 < args.Length
                        ? args[containerIndex + 1]
                        : null;

                    var account = new CloudStorageAccount(new StorageCredentials(accountName, accountKey), true);
                    if (containerName == null)
                    {
                        ProcessContinersAsync(
                            account.CreateCloudBlobClient(),
                            accountName,
                            accountKey,
                            sqlConnString)
                            .GetAwaiter().GetResult();
                    }
                    else
                    {
                        var container = account.CreateCloudBlobClient().GetContainerReference(containerName);
                        ProcessContainerAsync(
                            accountName,
                            accountKey,
                            container,
                            sqlConnString)
                            .GetAwaiter().GetResult();
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
            Console.WriteLine("Press any key...");
            Console.ReadKey();
        }

        private static async Task ProcessContinersAsync(
            CloudBlobClient blobClient,
            string blobAccountName,
            string blobAccountKey,
            string sqlConnString)
        {
            BlobContinuationToken token = null;
            while (true)
            {
                ContainerResultSegment containersResult = await blobClient.ListContainersSegmentedAsync(token);
                if (containersResult?.Results.Any() ?? false)
                    foreach (var container in containersResult.Results)
                    {
                        await ProcessContainerAsync(
                            blobAccountName,
                            blobAccountKey,
                            container,
                            sqlConnString);
                    }

                token = containersResult?.ContinuationToken;
                if (token == null)
                    break;
            }
        }

        private static async Task ProcessContainerAsync(
            string blobAccountName,
            string blobAccountKey,
            CloudBlobContainer container,
            string sqlConnString)
        {
            Console.WriteLine($"Processing container - {container.Name}");

            var tablesStructure = await GetStructureFromContainerAsync(container);
            if (tablesStructure == null)
                return;

            bool updateRequired = await CheckUpdateRequiredAsync(container);
            if (!updateRequired)
                return;

            var columnsListDict = GetColumnsListsFromStructure(tablesStructure);
            foreach (var tableStructure in tablesStructure.Tables)
            {
                Console.WriteLine($"\tSetting schema for table {tableStructure.TableName}");

                var sql = GenerateSqlCommand(
                    blobAccountName,
                    blobAccountKey,
                    tableStructure.TableName,
                    container.Name,
                    tableStructure.AzureBlobFolder,
                    columnsListDict[tableStructure.TableName]);
                int retryCount = 0;
                while (true)
                {
                    try
                    {
                        using (SqlConnection connection = new SqlConnection(sqlConnString))
                        {
                            await connection.OpenAsync();
                            SqlCommand command = new SqlCommand(sql, connection);
                            await command.ExecuteNonQueryAsync();
                        }
                        break;
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                        ++retryCount;
                        if (retryCount > _maxRetryCount)
                            throw;

                        await Task.Delay(TimeSpan.FromSeconds(retryCount));
                    }
                }
            }

            var updateBlob = container.GetBlockBlobReference(_structureUpdateFileName);
            await updateBlob.UploadTextAsync(string.Empty);
        }

        private static async Task<bool> CheckUpdateRequiredAsync(CloudBlobContainer container)
        {
            var updateBlob = container.GetBlockBlobReference(_structureUpdateFileName);
            if (!await updateBlob.ExistsAsync())
                return true;

            var structureBlob = container.GetBlockBlobReference(_structureFileName);
            if (!await structureBlob.ExistsAsync())
                return false;

            if (structureBlob.Properties?.Created == null)
                await structureBlob.FetchAttributesAsync();
            var structureDate = structureBlob.Properties.LastModified ?? structureBlob.Properties.Created;

            if (updateBlob.Properties?.Created == null)
                await updateBlob.FetchAttributesAsync();
            var updateDate = updateBlob.Properties.LastModified ?? updateBlob.Properties.Created;

            return structureDate > updateDate;
        }

        private static async Task<TablesStructure> GetStructureFromContainerAsync(CloudBlobContainer container)
        {
            var blob = container.GetBlockBlobReference(_structureFileName);
            if (!await blob.ExistsAsync())
                return null;

            string structureStr = await blob.DownloadTextAsync(null, _blobRequestOptions, null);
            return structureStr.DeserializeJson<TablesStructure>();
        }

        private static Dictionary<string, string> GetColumnsListsFromStructure(TablesStructure tablesStructure)
        {
            var result = new Dictionary<string, string>();

            foreach (var tableStructure in tablesStructure.Tables)
            {
                var strBuilder = new StringBuilder();
                var columns = tableStructure.Columns ?? tableStructure.Colums;
                foreach (var columnInfo in columns)
                {
                    if (strBuilder.Length > 0)
                        strBuilder.Append(", ");
                    strBuilder.Append($"[{columnInfo.ColumnName}] ");
                    if (columnInfo.ColumnType == typeof(DateTime).Name)
                        strBuilder.Append("DATETIME");
                    else if (columnInfo.ColumnType == typeof(double).Name || columnInfo.ColumnType == typeof(decimal).Name)
                        strBuilder.Append("Decimal");
                    else if (columnInfo.ColumnType == typeof(bool).Name)
                        strBuilder.Append("Bit");
                    else
                        strBuilder.Append("VARCHAR(256)");
                }

                result[tableStructure.TableName] = strBuilder.ToString();
            }

            return result;
        }

        private static string GenerateSqlCommand(
            string accountName,
            string accountKey,
            string tableName,
            string containerName,
            string blobFolder,
            string columnsList)
        {
            return $"exec CreateOrRepalceExternalTablev2 @StorageAccountName='{accountName}', @StorageAccountKey='{accountKey}', @containername='{containerName}'"
                + $", @TableName='{tableName}', @AzureBlobFolder='{blobFolder}', @ColumnList='{columnsList}', @FileFormat=NULL";
        }
    }
}
