using System.Collections.Generic;

namespace Lykke.Utility.DwhSchemaUpdater
{
    public class TableStructure
    {
        public string TableName { get; set; }

        public string AzureBlobFolder { get; set; }

        public List<ColumnInfo> Colums { get; set; }

        public List<ColumnInfo> Columns { get; set; }
    }
}
