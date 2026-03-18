using System.Text;

namespace Batchflow.EntityFrameworkExtensions.Internal;

internal static class BulkMergeCommandFactory
{
    public static BulkCommand BuildCommand<TEntity>(BulkTableModel table, IReadOnlyCollection<TEntity> entities)
        where TEntity : class
    {
        var sql = new StringBuilder();
        var parameters = new List<BulkSqlParameter>();

        AppendInsertStatement(sql, parameters, table, entities);
        sql.Append(" ON CONFLICT (");
        sql.Append(string.Join(", ", table.KeyColumns.Select(column => column.SqlColumnName)));
        sql.Append(") ");

        if (table.UpdateColumns.Count == 0)
        {
            sql.Append("DO NOTHING;");
        }
        else
        {
            sql.Append("DO UPDATE SET ");
            sql.Append(string.Join(", ", table.UpdateColumns.Select(column => $"{column.SqlColumnName} = excluded.{column.SqlColumnName}")));
            sql.Append(';');
        }

        return new BulkCommand(sql.ToString(), parameters.ToArray());
    }

    public static BulkCommand BuildInsertCommand<TEntity>(BulkTableModel table, IReadOnlyCollection<TEntity> entities)
        where TEntity : class
    {
        var sql = new StringBuilder();
        var parameters = new List<BulkSqlParameter>();

        AppendInsertStatement(sql, parameters, table, entities);
        sql.Append(';');

        return new BulkCommand(sql.ToString(), parameters.ToArray());
    }

    private static void AppendInsertStatement<TEntity>(StringBuilder sql, List<BulkSqlParameter> parameters, BulkTableModel table, IReadOnlyCollection<TEntity> entities)
        where TEntity : class
    {
        sql.Append("INSERT INTO ");
        sql.Append(table.QualifiedTableName);
        sql.Append(" (");
        sql.Append(string.Join(", ", table.InsertColumns.Select(column => column.SqlColumnName)));
        sql.Append(") VALUES ");

        var parameterIndex = 0;
        var rowIndex = 0;

        foreach (var entity in entities)
        {
            if (rowIndex > 0)
            {
                sql.Append(", ");
            }

            sql.Append('(');

            for (var columnIndex = 0; columnIndex < table.InsertColumns.Count; columnIndex++)
            {
                if (columnIndex > 0)
                {
                    sql.Append(", ");
                }

                var column = table.InsertColumns[columnIndex];
                var value = column.GetWriteValue(entity);

                sql.Append("@p");
                sql.Append(parameterIndex);
                parameters.Add(new BulkSqlParameter($"@p{parameterIndex}", value));
                parameterIndex++;
            }

            sql.Append(')');
            rowIndex++;
        }
    }
}
