﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Extensions.Streams;
using Raven.Client.Util;
using Raven.Server.Documents.ETL.Providers.RelationalDatabase.Common.Metrics;
using Raven.Server.Documents.ETL.Providers.RelationalDatabase.SQL;
using Raven.Server.NotificationCenter.Notifications.Details;
using Sparrow;
using Sparrow.Json;
using Sparrow.Logging;
using DbProviderFactories = System.Data.Common.DbProviderFactories;

namespace Raven.Server.Documents.ETL.Providers.RelationalDatabase.Common.RelationalWriters;

public abstract class RelationalDatabaseWriterBase<TRelationalConnectionString, TRelationalEtlConfiguration> : IDisposable, IRelationalDatabaseWriter
    where TRelationalConnectionString : ConnectionString
    where TRelationalEtlConfiguration : EtlConfiguration<TRelationalConnectionString>
{
    protected readonly Logger Logger;
    protected readonly DocumentDatabase Database;
    private readonly DbCommandBuilder _commandBuilder;
    protected readonly DbProviderFactory ProviderFactory;
    private readonly DbConnection _connection;
    private readonly DbTransaction _tx;
    private readonly RelationalDatabaseEtlMetricsCountersManager _sqlMetrics;
    private readonly EtlProcessStatistics _statistics;
    private readonly string _etlName;
    protected readonly EtlConfiguration<TRelationalConnectionString> Configuration;
    private readonly List<Func<DbParameter, string, bool>> _stringParserList;
    private const int LongStatementWarnThresholdInMs = 3000;

    protected RelationalDatabaseWriterBase(DocumentDatabase database, EtlConfiguration<TRelationalConnectionString> configuration,
        RelationalDatabaseEtlMetricsCountersManager sqlMetrics, EtlProcessStatistics statistics, bool shouldConnectToTarget = true)
    {
        _sqlMetrics = sqlMetrics;
        _statistics = statistics;
        _etlName = configuration.Name;

        Database = database;
        ProviderFactory = GetDbProviderFactory(configuration);
        Configuration = configuration;
        _connection = ProviderFactory.CreateConnection();
        _commandBuilder = GetInitializedCommandBuilder();
        Logger = LoggingSource.Instance
            .GetLogger<
                RelationalDatabaseWriterBase<TRelationalConnectionString, TRelationalEtlConfiguration>>(Database.Name);

        var connectionString = GetConnectionString(configuration);
        _connection.ConnectionString = connectionString;

        if (shouldConnectToTarget)
        {
            OpenConnection(database, configuration.Name, configuration.ConnectionStringName);
            _tx = _connection.BeginTransaction();
            _stringParserList = GenerateStringParsers();
        }
    }

    public abstract bool ParametrizeDeletes { get; }

    protected abstract string GetConnectionString(EtlConfiguration<TRelationalConnectionString> configuration);

    protected abstract DbCommandBuilder GetInitializedCommandBuilder();

    protected abstract DbProviderFactory GetDbProviderFactory(EtlConfiguration<TRelationalConnectionString> configuration);

    private void OpenConnection(DocumentDatabase database, string etlConfigurationName, string connectionStringName)
    {
        const int maxRetries = 5;
        var retries = 0;

        while (true)
        {
            try
            {
                _connection.Open();
                return;
            }
            catch (Exception e)
            {
                if (++retries < maxRetries)
                {
                    if (Logger.IsInfoEnabled)
                        Logger.Info($"Failed to open connection, retrying ({retries}/{maxRetries})", e);

                    Thread.Sleep(50);
                    continue;
                }

                using (_connection)
                {
                    CreateAlertCannotOpenConnection(database, etlConfigurationName, connectionStringName, e);
                    throw;
                }
            }
        }
    }

    protected abstract void CreateAlertCannotOpenConnection(DocumentDatabase database, string etlConfigurationName, string connectionStringName, Exception e);

    public static void TestConnection(string factoryName, string connectionString)
    {
        var providerFactory = DbProviderFactories.GetFactory(factoryName);
        var connection = providerFactory.CreateConnection();
        connection.ConnectionString = connectionString;

        try
        {
            connection.Open();
            connection.Close();
        }
        finally
        {
            connection.Dispose();
        }
    }

    public void Dispose()
    {
        using (_connection)
        using (_tx)
        {

        }
    }

    public void Commit()
    {
        _tx.Commit();
    }

    public void Rollback()
    {
        _tx.Rollback();
    }

    private int InsertItems(string tableName, string pkName, List<ToRelationalDatabaseItem> toInsert, Action<DbCommand> commandCallback, CancellationToken token)
    {
        var inserted = 0;

        var sp = new Stopwatch();
        foreach (var itemToReplicate in toInsert)
        {
            sp.Restart();

            using (var cmd = GetInsertCommand(tableName, pkName, itemToReplicate))
            using (token.Register(cmd.Cancel))
            {
                token.ThrowIfCancellationRequested();
                commandCallback?.Invoke(cmd);

                try
                {
                    cmd.ExecuteNonQuery();
                    inserted++;
                }
                catch (Exception e)
                {
                    if (token.IsCancellationRequested == false)
                    {
                        if (Logger.IsInfoEnabled)
                        {
                            Logger.Info(
                                $"Failed to replicate changes to relational database for: {_etlName} " +
                                $"(doc: {itemToReplicate.DocumentId}), will continue trying. {Environment.NewLine}{cmd.CommandText}", e);
                        }

                        _statistics.RecordPartialLoadError(
                            $"Insert statement:{Environment.NewLine}{cmd.CommandText}{Environment.NewLine}. Error:{Environment.NewLine}{e}",
                            itemToReplicate.DocumentId);
                    }
                }
                finally
                {
                    sp.Stop();

                    var elapsedMilliseconds = sp.ElapsedMilliseconds;

                    if (Logger.IsInfoEnabled && token.IsCancellationRequested == false)
                        Logger.Info($"Insert took: {elapsedMilliseconds:#,#;;0}ms, statement: {cmd.CommandText}");

                    var tableMetrics = _sqlMetrics.GetTableMetrics(tableName);
                    tableMetrics.InsertActionsMeter.MarkSingleThreaded(1);

                    if (elapsedMilliseconds > LongStatementWarnThresholdInMs)
                    {
                        HandleSlowSql(elapsedMilliseconds, cmd.CommandText);
                    }
                }
            }
        }

        return inserted;
    }

    protected abstract int? GetCommandTimeout();

    protected DbCommand CreateCommand()
    {
        var cmd = _connection.CreateCommand();

        try
        {
            cmd.Transaction = _tx;

            var commandTimeout = GetCommandTimeout();
            if (commandTimeout.HasValue)
                cmd.CommandTimeout = commandTimeout.Value;
            else if (Database.Configuration.Etl.SqlCommandTimeout.HasValue)
                cmd.CommandTimeout = (int)Database.Configuration.Etl.SqlCommandTimeout.Value.AsTimeSpan.TotalSeconds;

            return cmd;
        }
        catch (Exception)
        {
            cmd.Dispose();
            throw;
        }
    }

    public int DeleteItems(string tableName, string pkName, bool parameterize, List<ToRelationalDatabaseItem> toDelete, Action<DbCommand> commandCallback,
        CancellationToken token)
    {
        const int maxParams = 1000;
        var deleted = 0;

        var sp = new Stopwatch();
        sp.Start();

        token.ThrowIfCancellationRequested();

        for (int i = 0; i < toDelete.Count; i += maxParams)
        {
            using (var cmd = GetDeleteCommand(tableName, pkName, toDelete, i, parameterize, maxParams, out int countOfDeletes))
            using (token.Register(cmd.Cancel))
            {
                commandCallback?.Invoke(cmd);

                try
                {
                    cmd.ExecuteNonQuery();

                    deleted += countOfDeletes;
                }
                catch (Exception e)
                {
                    if (token.IsCancellationRequested == false)
                    {
                        if (Logger.IsInfoEnabled)
                            Logger.Info($"Failure to replicate deletions to relational database for: {_etlName}, " +
                                        "will continue trying." + Environment.NewLine + cmd.CommandText, e);

                        _statistics.RecordPartialLoadError(
                            $"Delete statement:{Environment.NewLine}{cmd.CommandText}{Environment.NewLine}Error:{Environment.NewLine}{e}",
                            null);
                    }
                }
                finally
                {
                    sp.Stop();

                    var elapsedMilliseconds = sp.ElapsedMilliseconds;

                    if (Logger.IsInfoEnabled && token.IsCancellationRequested == false)
                        Logger.Info($"Delete took: {elapsedMilliseconds:#,#;;0}ms, statement: {cmd.CommandText}");

                    var tableMetrics = _sqlMetrics.GetTableMetrics(tableName);
                    tableMetrics.DeleteActionsMeter.MarkSingleThreaded(1);

                    if (elapsedMilliseconds > LongStatementWarnThresholdInMs)
                    {
                        HandleSlowSql(elapsedMilliseconds, cmd.CommandText);
                    }
                }
            }
        }

        return deleted;
    }

    private void HandleSlowSql(long elapsedMilliseconds, string stmt)
    {
        if (Logger.IsInfoEnabled)
            Logger.Info($"[{_etlName}] Slow SQL detected. Execution took: {elapsedMilliseconds:#,#;;0}ms, statement: {stmt}");

        _statistics.RecordSlowSql(new SlowSqlStatementInfo { Date = SystemTime.UtcNow, Duration = elapsedMilliseconds, Statement = stmt });
    }

    protected abstract bool ShouldQuoteTables();

    private string GetTableNameString(string tableName)
    {
        if (ShouldQuoteTables())
        {
            return string.Join(".", tableName.Split('.').Select(_commandBuilder.QuoteIdentifier).ToArray());
        }

        return tableName;
    }

    public static string SanitizeSqlValue(string sqlValue)
    {
        return sqlValue.Replace("'", "''");
    }

    protected abstract string GetParameterNameForDbParameter(string paramName);

    protected abstract string GetParameterNameForCommandString(string targetParamName, bool parseJson);

    protected abstract void EnsureParamTypeSupportedByDbProvider(DbParameter parameter);

    protected abstract void SetPrimaryKeyParamValue(ToRelationalDatabaseItem itemToReplicate, DbParameter pkParam);

    protected abstract string GetPostInsertIntoStartSyntax(ToRelationalDatabaseItem itemToReplicate);
    protected abstract string GetPostInsertIntoEndSyntax(ToRelationalDatabaseItem itemToReplicate);

    protected abstract string GetPostDeleteSyntax(ToRelationalDatabaseItem itemToDelete);

    public RelationalDatabaseWriteStats Write(RelationalDatabaseTableWithRecords table, List<DbCommand> commands, CancellationToken token)
    {
        var stats = new RelationalDatabaseWriteStats();

        var collectCommands = commands != null ? commands.Add : (Action<DbCommand>)null;

        if (table.InsertOnlyMode == false && table.Deletes.Count > 0)
        {
            // first, delete all the rows that might already exist there
            stats.DeletedRecordsCount = DeleteItems(table.TableName, table.DocumentIdColumn, ParametrizeDeletes, table.Deletes, collectCommands,
                token);
        }

        if (table.Inserts.Count > 0)
        {
            stats.InsertedRecordsCount = InsertItems(table.TableName, table.DocumentIdColumn, table.Inserts, collectCommands, token);
        }

        return stats;
    }

    protected abstract void HandleCustomDbTypeObject(DbParameter colParam, RelationalDatabaseColumn column, object dbType, object fieldValue,
        BlittableJsonReaderObject objectValue);

    public void SetParamValue(DbParameter colParam, RelationalDatabaseColumn column, List<Func<DbParameter, string, bool>> stringParsers)
    {
        if (column.Value == null)
            // we'll end up here e.g. if we can't find attachment loaded from the script - the value will be null 
            colParam.Value = DBNull.Value;
        else
        {
            switch (column.Type & BlittableJsonReaderBase.TypesMask)
            {
                case BlittableJsonToken.Null:
                    colParam.Value = DBNull.Value;
                    break;
                case BlittableJsonToken.Boolean:
                case BlittableJsonToken.Integer:
                    colParam.Value = column.Value;
                    break;
                case BlittableJsonToken.LazyNumber:
                    colParam.Value = (double)(LazyNumberValue)column.Value;
                    break;

                case BlittableJsonToken.String:
                    SetParamStringValue(colParam, ((LazyStringValue)column.Value).ToString(), stringParsers);
                    break;
                case BlittableJsonToken.CompressedString:
                    SetParamStringValue(colParam, ((LazyCompressedStringValue)column.Value).ToString(), stringParsers);
                    break;

                case BlittableJsonToken.StartObject:
                    var objectValue = (BlittableJsonReaderObject)column.Value;
                    if (objectValue.Count >= 2)
                    {
                        if (objectValue.TryGetMember(nameof(SqlDocumentTransformer.VarcharFunctionCall.Type), out object dbType) &&
                            objectValue.TryGetMember(nameof(SqlDocumentTransformer.VarcharFunctionCall.Value), out object fieldValue))
                        {
                            HandleCustomDbTypeObject(colParam, column, dbType, fieldValue, objectValue);
                            break;
                        }
                    }

                    colParam.Value = objectValue.ToString();
                    break;
                case BlittableJsonToken.StartArray:
                    var blittableJsonReaderArray = (BlittableJsonReaderArray)column.Value;
                    colParam.Value = blittableJsonReaderArray.ToString();
                    break;
                default:
                {
                    if (column.Value is Stream stream)
                    {
                        colParam.DbType = DbType.Binary;

                        if (stream == Stream.Null)
                            colParam.Value = DBNull.Value;
                        else
                            colParam.Value = stream.ReadData();

                        break;
                    }

                    throw new InvalidOperationException("Cannot understand how to save " + column.Type + " for " + colParam.ParameterName);
                }
            }
        }
    }

    private static void SetParamStringValue(DbParameter colParam, string value, List<Func<DbParameter, string, bool>> stringParsers)
    {
        if (value.Length > 0 && stringParsers != null)
        {
            foreach (var parser in stringParsers)
            {
                if (parser(colParam, value))
                {
                    return;
                }
            }
        }

        colParam.Value = value;
    }

    public List<Func<DbParameter, string, bool>> GenerateStringParsers()
    {
        return new List<Func<DbParameter, string, bool>>
        {
            (colParam, value) =>
            {
                if (char.IsDigit(value[0]))
                {
                    if (DateTime.TryParseExact(value, DefaultFormat.OnlyDateTimeFormat, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind,
                            out DateTime dateTime))
                    {
                        colParam.Value = dateTime;
                        return true;
                    }
                }

                return false;
            },
            (colParam, value) =>
            {
                if (char.IsDigit(value[0]))
                {
                    if (DateTimeOffset.TryParseExact(value, DefaultFormat.DateTimeFormatsToRead, CultureInfo.InvariantCulture,
                            DateTimeStyles.RoundtripKind, out DateTimeOffset dateTimeOffset))
                    {
                        colParam.Value = dateTimeOffset;
                        return true;
                    }
                }

                return false;
            }
        };
    }

    public DbCommand GetInsertCommand(string tableName, string pkName, ToRelationalDatabaseItem itemToReplicate)
    {
        var cmd = CreateCommand();
        
        var sb = new StringBuilder("INSERT INTO ")
            .Append(GetTableNameString(tableName))
            .Append(" (")
            .Append(_commandBuilder.QuoteIdentifier(pkName))
            .Append(", ");
        foreach (var column in itemToReplicate.Columns)
        {
            if (column.Id == pkName)
                continue;
            sb.Append(_commandBuilder.QuoteIdentifier(column.Id)).Append(", ");
        }

        sb.Length = sb.Length - 2;

        var pkParam = cmd.CreateParameter();
        pkParam.ParameterName = GetParameterNameForDbParameter(pkName);

        SetPrimaryKeyParamValue(itemToReplicate, pkParam);
        cmd.Parameters.Add(pkParam);

        var afterIntoSyntax = GetPostInsertIntoStartSyntax(itemToReplicate);

        sb.Append($") {afterIntoSyntax}");

        sb.Append(GetParameterNameForCommandString(pkName, false)).Append(", ");

        foreach (var column in itemToReplicate.Columns)
        {
            if (column.Id == pkName)
                continue;

            var colParam = cmd.CreateParameter();
            colParam.ParameterName = GetParameterNameForDbParameter(column.Id);
            SetParamValue(colParam, column, _stringParserList);
            EnsureParamTypeSupportedByDbProvider(colParam);

            cmd.Parameters.Add(colParam);
            sb.Append(GetParameterNameForCommandString(column.Id, column.IsArrayOrObject)).Append(", ");
        }

        sb.Length = sb.Length - 2;
        var endSyntax = GetPostInsertIntoEndSyntax(itemToReplicate);
        sb.Append(endSyntax);

        var stmt = sb.ToString();
        cmd.CommandText = stmt;

        return cmd;
    }

    public DbCommand GetDeleteCommand(string tableName, string pkName, List<ToRelationalDatabaseItem> toDeleteSqlItems, int currentToDeleteIndex, bool parameterize, int maxParams, out int countOfDeletes)
    {
        var cmd = CreateCommand();
        cmd.Parameters.Clear();
        var sb = new StringBuilder("DELETE FROM ")
            .Append(GetTableNameString(tableName))
            .Append(" WHERE ")
            .Append(_commandBuilder.QuoteIdentifier(pkName))
            .Append(" IN (");

        countOfDeletes = 0;
        for (int j = currentToDeleteIndex; j < Math.Min(currentToDeleteIndex + maxParams, toDeleteSqlItems.Count); j++)
        {
            if (currentToDeleteIndex != j)
                sb.Append(", ");

            if (parameterize)
            {
                var dbParameter = cmd.CreateParameter();
                dbParameter.ParameterName = GetParameterNameForCommandString("p" + j, false);
                dbParameter.Value = toDeleteSqlItems[j].DocumentId.ToString();
                cmd.Parameters.Add(dbParameter);
                sb.Append(dbParameter.ParameterName);
            }
            else
            {
                sb.Append('\'').Append(SanitizeSqlValue(toDeleteSqlItems[j].DocumentId)).Append('\'');
            }

            if (toDeleteSqlItems[j].IsDelete) // count only "real" deletions, not the ones because of insert
                countOfDeletes++;
        }

        sb.Append(')');

        var endSyntax = GetPostDeleteSyntax(toDeleteSqlItems[currentToDeleteIndex]);
        sb.Append(endSyntax);


        var stmt = sb.ToString();
        cmd.CommandText = stmt;
        return cmd;
    }
}

