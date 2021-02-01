using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Data;
using System.Data.SqlClient;
using CsvHelper;
using CsvHelper.Configuration;

namespace test
{
    class email_research_news
    {
        static void Main(string[] args)
        {
            using (SqlConnection dbConnection = new SqlConnection(@"Data Source=na-d-sql02\sql2012,4334;Initial Catalog=SCV;Integrated Security=True;"))
            {
                dbConnection.Open();
                using (SqlBulkCopy s = new SqlBulkCopy(dbConnection))
                {
                    List<dynamic> rows;
                    List<string> columns;
                    using (var reader = new StreamReader(@"C:\Temp\Email_Research_News_dynamic_comma.csv"))
                    using (var csv = new CsvReader(reader, System.Globalization.CultureInfo.CurrentCulture)) // added CultureInfo.CurrentCulture
                    {
                        rows = csv.GetRecords<dynamic>().ToList();
                        columns = csv.Context.HeaderRecord.ToList();
                    }

                    if (rows.Count == 0)
                        return;
                    var table = new DataTable();
                    s.ColumnMappings.Clear();
                    foreach (var c in columns)
                    {
                        table.Columns.Add(c);
                        s.ColumnMappings.Add(c, c);
                    }

                    foreach (IDictionary<string, object> row in rows)
                    {
                        var rowValues = row.Values
                        .Select(a => string.IsNullOrEmpty(a.ToString()) ? null : a)
                        .ToArray();
                        table.Rows.Add(rowValues);
                    }
                    s.DestinationTableName = "dbo.[staging_econnect_email_research_news]";
                    s.WriteToServer(table);
                }
            }
        }
    }
}