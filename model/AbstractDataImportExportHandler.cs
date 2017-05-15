using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using SchemaZen.Library.Models;

namespace SchemaZen.Library {
    public abstract class AbstractDataImportExportHandler : IDataImportExportHandler
    {
        public static readonly int RowsInBatch = 15000;
        public abstract string FileExtension { get; }

        public void ExportData(Table table, string conn, TextWriter data, string tableHint = null)
        {
            if (table.IsType)
                throw new InvalidOperationException();

            var sql = new StringBuilder();
            sql.Append("select ");
            var cols = table.Columns.Items.Where(c => string.IsNullOrEmpty(c.ComputedDefinition)).ToArray();
            foreach (var c in cols)
            {
                sql.Append($"[{c.Name}],");
            }
            sql.Remove(sql.Length - 1, 1);
            sql.Append($" from [{table.Owner}].[{table.Name}]");

            if (!string.IsNullOrEmpty(tableHint))
                sql.Append($" WITH ({tableHint})");

	        AppendOrderBy(table, sql, cols);

			using (var cn = new SqlConnection(conn))
            {
                cn.Open();
                using (var cm = cn.CreateCommand())
                {
                    cm.CommandText = sql.ToString();
                    using (var dr = cm.ExecuteReader())
                    {
                        ExportData(dr, cols, data);
                    }
                }
            }
        }

	    private void AppendOrderBy(Table table, StringBuilder sql, IEnumerable<Column> cols)
	    {
		    sql.Append(" ORDER BY ");

		    if (table.PrimaryKey != null)
		    {
			    var pkColumns = table.PrimaryKey.Columns.Select(c => $"[{c.ColumnName}]");
			    sql.Append(string.Join(",", pkColumns.ToArray()));
			    return;
		    }

		    var uk = table.Constraints.Where(c => c.Unique).OrderBy(c => c.Columns.Count).ThenBy(c => c.Name).FirstOrDefault();

		    if (uk != null)
		    {
			    var ukColumns = uk.Columns.Select(c => $"[{c.ColumnName}]");
			    sql.Append(string.Join(",", ukColumns.ToArray()));
			    return;
		    }

		    var allColumns = cols.Select(c => $"[{c.Name}]");
		    sql.Append(string.Join(",", allColumns.ToArray()));
	    }

		public void ImportData(Table table, string conn, string filename)
        {
            if (table.IsType)
                throw new InvalidOperationException();

            var dt = new DataTable();
            var cols = table.Columns.Items.Where(c => string.IsNullOrEmpty(c.ComputedDefinition)).ToArray();
            foreach (var c in cols)
            {
                dt.Columns.Add(new DataColumn(c.Name, c.SqlTypeToNativeType()));
            }

            using (var bulk = new SqlBulkCopy(conn, SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.KeepNulls | SqlBulkCopyOptions.TableLock))
            {
                foreach (var colName in dt.Columns.OfType<DataColumn>().Select(c => c.ColumnName))
                    bulk.ColumnMappings.Add(colName, colName);

                bulk.DestinationTableName = $"[{table.Owner}].[{table.Name}]";

                ImportData(dt, cols, bulk, filename);
            }
        }

        protected abstract void ExportData(SqlDataReader dr, Column[] cols, TextWriter data);

        protected abstract void ImportData(DataTable dt, Column[] cols, SqlBulkCopy bulk, string filename);
    }
}