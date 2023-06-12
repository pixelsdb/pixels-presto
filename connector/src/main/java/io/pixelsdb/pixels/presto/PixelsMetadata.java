/*
 * Copyright 2019 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.presto;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.statistics.DoubleRange;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.TimestampWithTimeZoneType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.*;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.statistics.ColumnStatistics;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.InvalidProtocolBufferException;
import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.metadata.domain.Column;
import io.pixelsdb.pixels.common.metadata.domain.View;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.core.PixelsProto;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.stats.RangeStats;
import io.pixelsdb.pixels.core.stats.StatsRecorder;
import io.pixelsdb.pixels.presto.exception.PixelsErrorCode;
import io.pixelsdb.pixels.presto.impl.PixelsMetadataProxy;

import javax.inject.Inject;
import java.nio.ByteBuffer;
import java.util.*;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateTimeEncoding.unpackMillisUtc;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.Decimals.longTenToNth;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.google.common.base.Preconditions.checkArgument;
import static io.pixelsdb.pixels.presto.properties.PixelsTableProperties.PATHS;
import static io.pixelsdb.pixels.presto.properties.PixelsTableProperties.STORAGE;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * @author: tao
 * @author hank
 * @date: Create in 2018-01-19 14:16
 **/
public class PixelsMetadata
        implements ConnectorMetadata
{
    private static final Logger logger = Logger.get(PixelsMetadata.class);
    // private static final Logger logger = Logger.get(PixelsMetadata.class);
    private final String connectorId;

    private final PixelsMetadataProxy metadataProxy;

    @Inject
    public PixelsMetadata(PixelsConnectorId connectorId, PixelsMetadataProxy metadataProxy)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.metadataProxy = requireNonNull(metadataProxy, "metadataProxy is null");
    }

    @Override
    public boolean schemaExists(ConnectorSession session, String schemaName)
    {
        try
        {
            return this.metadataProxy.existSchema(schemaName);
        } catch (MetadataException e)
        {
            throw new PrestoException(PixelsErrorCode.PIXELS_METASTORE_ERROR, e);
        }
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return listSchemaNamesInternal();
    }

    private List<String> listSchemaNamesInternal()
    {
        List<String> schemaNameList = null;
        try
        {
            schemaNameList = metadataProxy.getSchemaNames();
        } catch (MetadataException e)
        {
            throw new PrestoException(PixelsErrorCode.PIXELS_METASTORE_ERROR, e);
        }
        return schemaNameList;
    }

    @Override
    public PixelsTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        requireNonNull(tableName, "tableName is null");
        try
        {
            if (this.metadataProxy.existTable(tableName.getSchemaName(), tableName.getTableName()))
            {
                PixelsTableHandle tableHandle = new PixelsTableHandle(
                        connectorId, tableName.getSchemaName(), tableName.getTableName(), "");
                return tableHandle;
            }
        } catch (MetadataException e)
        {
            throw new PrestoException(PixelsErrorCode.PIXELS_METASTORE_ERROR, e);
        }
        return null;
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table,
                                                            Constraint<ColumnHandle> constraint,
                                                            Optional<Set<ColumnHandle>> desiredColumns)
    {
        PixelsTableHandle tableHandle = (PixelsTableHandle) table;
        PixelsTableLayoutHandle tableLayout = new PixelsTableLayoutHandle(tableHandle);
        tableLayout.setConstraint(constraint.getSummary());
        if(desiredColumns.isPresent())
            tableLayout.setDesiredColumns(desiredColumns.get());
        ConnectorTableLayout layout = getTableLayout(session, tableLayout);
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        return new ConnectorTableLayout(handle);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        PixelsTableHandle tableHandle = (PixelsTableHandle) table;
        checkArgument(tableHandle.getConnectorId().equals(connectorId),
                "tableHandle is not for this connector");
        return getTableMetadataInternal(tableHandle.getSchemaName(), tableHandle.getTableName());
    }

    private ConnectorTableMetadata getTableMetadataInternal(String schemaName, String tableName)
    {
        List<PixelsColumnHandle> columnHandleList;
        try
        {
            columnHandleList = metadataProxy.getTableColumn(connectorId, schemaName, tableName);
        } catch (MetadataException e)
        {
            throw new PrestoException(PixelsErrorCode.PIXELS_METASTORE_ERROR, e);
        }
        List<ColumnMetadata> columns = columnHandleList.stream().map(PixelsColumnHandle::getColumnMetadata)
                .collect(toList());
        return new ConnectorTableMetadata(new SchemaTableName(schemaName, tableName), columns);
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle table,
                                              Optional<ConnectorTableLayoutHandle> tableLayoutHandle,
                                              List<ColumnHandle> columnHandles, Constraint<ColumnHandle> constraint)
    {
        TableStatistics.Builder tableStatBuilder = TableStatistics.builder();
        PixelsTableHandle tableHandle = (PixelsTableHandle) table;
        List<PixelsColumnHandle> pixelsColumns = columnHandles.stream()
                .map(PixelsColumnHandle.class::cast).collect(toList());
        List<Column> columns = metadataProxy.getColumnStatistics(tableHandle.getSchemaName(), tableHandle.getTableName());
        requireNonNull(columns, "columns is null");
        Map<String, Column> columnMap = new HashMap<>(columns.size());
        for (Column column : columns)
        {
            columnMap.put(column.getName(), column);
        }

        try
        {
            long rowCount = metadataProxy.getTable(tableHandle.getSchemaName(), tableHandle.getTableName()).getRowCount();
            tableStatBuilder.setRowCount(Estimate.of(rowCount));
            logger.debug("table '" + tableHandle.getTableName() + "' row count: " + rowCount);
        } catch (MetadataException e)
        {
            logger.error(e, "failed to get table from metadata service");
            throw new PrestoException(PixelsErrorCode.PIXELS_METASTORE_ERROR, e);
        }

        for (PixelsColumnHandle columnHandle : pixelsColumns)
        {
            ColumnStatistics.Builder columnStatsBuilder = ColumnStatistics.builder();
            Column column = columnMap.get(columnHandle.getColumnName());
            columnStatsBuilder.setDataSize(Estimate.of(column.getSize()));
            columnStatsBuilder.setNullsFraction(Estimate.of(column.getNullFraction()));
            columnStatsBuilder.setDistinctValuesCount(Estimate.of(column.getCardinality()));
            try
            {
                TypeDescription pixelsType = metadataProxy.parsePixelsType(columnHandle.getColumnType());
                ByteBuffer statsBytes = column.getRecordStats().slice();
                if (statsBytes != null && statsBytes.remaining() > 0)
                {
                    PixelsProto.ColumnStatistic columnStatsPb = PixelsProto.ColumnStatistic.parseFrom(statsBytes);
                    StatsRecorder statsRecorder = StatsRecorder.create(pixelsType, columnStatsPb);
                    if (statsRecorder instanceof RangeStats)
                    {
                        /**
                         * When needed, we can also get a general range stats like this:
                         * GeneralRangeStats rangeStats = StatsRecorder.toGeneral(pixelsType, (RangeStats<?>) statsRecorder);
                         * The double min/max in general range stats is the readable representation of the min/max value.
                         */
                        RangeStats<?> rangeStats = (RangeStats<?>) statsRecorder;
                        logger.debug(column.getName() + " column range: {min:" + rangeStats.getMinimum() + ", max:" +
                                rangeStats.getMaximum() + ", hasMin:" + rangeStats.hasMinimum() +
                                ", hasMax:" + rangeStats.hasMaximum() + "}");
                        Type columnType = columnHandle.getColumnType();
                        OptionalDouble min = statValueToDouble(columnType, rangeStats.getMinimum());
                        OptionalDouble max = statValueToDouble(columnType, rangeStats.getMaximum());
                        if (min.isPresent() && max.isPresent())
                        {
                            columnStatsBuilder.setRange(new DoubleRange(min.getAsDouble(), max.getAsDouble()));
                        }
                    }
                }
            } catch (InvalidProtocolBufferException e)
            {
                logger.error(e, "failed to parse record statistics from protobuf object");
            }
            tableStatBuilder.setColumnStatistics(columnHandle, columnStatsBuilder.build());
        }

        return tableStatBuilder.build();
    }

    /**
     * Convert the min / max value of ColumnStats to double value.
     * This method refers to {@code io.trino.spi.statistics.StatsUtil.toStatsRepresentation(Type type, Object value)}.
     * @param type the column type.
     * @param value the min / max value of ColumnStats.
     * @return the double value, may be empty if the conversion is not supported.
     */
    private static OptionalDouble statValueToDouble(Type type, Object value)
    {
        requireNonNull(type, "type is null");
        requireNonNull(value, "value is null");

        if (type == BOOLEAN)
        {
            return OptionalDouble.of((boolean) value ? 1 : 0);
        }
        if (type == TINYINT || type == SMALLINT || type == INTEGER || type == BIGINT)
        {
            return OptionalDouble.of((long) value);
        }
        if (type == REAL)
        {
            return OptionalDouble.of(intBitsToFloat(toIntExact((Long) value)));
        }
        if (type == DOUBLE)
        {
            return OptionalDouble.of((double) value);
        }
        if (type instanceof DecimalType)
        {
            DecimalType decimalType = (DecimalType) type;
            if (decimalType.isShort())
            {
                return OptionalDouble.of(shortDecimalToDouble((long) value, longTenToNth(decimalType.getScale())));
            }
            // Issue #24: we currently do not support get column statistics for long decimal columns.
            return OptionalDouble.empty();
        }
        if (type == DATE)
        {
            return OptionalDouble.of((long) value);
        }
        if (type instanceof TimestampType)
        {
            return OptionalDouble.of((long) value);
        }
        if (type instanceof TimestampWithTimeZoneType)
        {
            return OptionalDouble.of(unpackMillisUtc((long) value));
        }

        return OptionalDouble.empty();
    }

    private static double shortDecimalToDouble(long decimal, long tenToScale)
    {
        return ((double) decimal) / tenToScale;
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        try
        {
            List<String> schemaNames;
            if (schemaName.isPresent())
            {
                schemaNames = ImmutableList.of(schemaName.get());
            } else
            {
                schemaNames = metadataProxy.getSchemaNames();
            }

            ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
            for (String schema : schemaNames)
            {
                /**
                 * PIXELS-179:
                 * Only try to get table names if the schema exists.
                 * 'show tables' in information_schema also invokes this method.
                 * In this case, information_schema does not exist in the metadata,
                 * we should return an empty list without throwing an exception.
                 * Presto will add the system tables by itself.
                 */
                if (metadataProxy.existSchema(schema))
                {
                    for (String table : metadataProxy.getTableNames(schema))
                    {
                        builder.add(new SchemaTableName(schema, table));
                    }
                }
            }
            return builder.build();
        } catch (MetadataException e)
        {
            throw new PrestoException(PixelsErrorCode.PIXELS_METASTORE_ERROR, e);
        }
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        PixelsTableHandle pixelsTableHandle = (PixelsTableHandle) tableHandle;
        checkArgument(pixelsTableHandle.getConnectorId().equals(connectorId),
                "tableHandle is not for this connector");

        List<PixelsColumnHandle> columnHandleList = null;
        try
        {
            columnHandleList = metadataProxy.getTableColumn(
                    connectorId, pixelsTableHandle.getSchemaName(), pixelsTableHandle.getTableName());
        } catch (MetadataException e)
        {
            throw new PrestoException(PixelsErrorCode.PIXELS_METASTORE_ERROR, e);
        }
        if (columnHandleList == null)
        {
            throw new TableNotFoundException(pixelsTableHandle.toSchemaTableName());
        }

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        for (PixelsColumnHandle column : columnHandleList)
        {
            columnHandles.put(column.getColumnName(), column);
        }
        return columnHandles.build();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session,
                                                                       SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        if (prefix.getSchemaName() != null)
        {
            SchemaTableName tableName = new SchemaTableName(prefix.getSchemaName(), prefix.getTableName());
            try
            {
                /**
                 * PIXELS-183:
                 * Return an empty result if the table does not exist.
                 * This is possible when reading the content of information_schema tables.
                 */
                if (metadataProxy.existTable(tableName.getSchemaName(), tableName.getTableName()))
                {
                    ConnectorTableMetadata tableMetadata = getTableMetadataInternal(
                            tableName.getSchemaName(), tableName.getTableName());
                    // table can disappear during listing operation
                    if (tableMetadata != null)
                    {
                        columns.put(tableName, tableMetadata.getColumns());
                    }
                }
            } catch (MetadataException e)
            {
                throw new PrestoException(PixelsErrorCode.PIXELS_METASTORE_ERROR, e);
            }
        }
        return columns.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle,
                                            ColumnHandle columnHandle)
    {
        return ((PixelsColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        SchemaTableName schemaTableName = tableMetadata.getTable();
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();
        String storage = (String) tableMetadata.getProperties().get(STORAGE);
        String paths = (String) tableMetadata.getProperties().get(PATHS);
        if (storage == null)
        {
            throw new PrestoException(PixelsErrorCode.PIXELS_QUERY_PARSING_ERROR,
                    "Table must be created with the property 'storage'.");
        }
        if (paths == null)
        {
            throw new PrestoException(PixelsErrorCode.PIXELS_QUERY_PARSING_ERROR,
                    "Table must be created with the property 'paths'.");
        }
        if (!Storage.Scheme.isValid(storage))
        {
            throw new PrestoException(PixelsErrorCode.PIXELS_QUERY_PARSING_ERROR,
                    "Unsupported storage scheme '" + storage + "'.");
        }
        Storage.Scheme storageScheme = Storage.Scheme.from(storage);
        String[] basePathUris = paths.split(";");
        for (int i = 0; i < basePathUris.length; ++i)
        {
            Storage.Scheme scheme = Storage.Scheme.fromPath(basePathUris[i]);
            if (scheme == null)
            {
                basePathUris[i] = storageScheme + "://" + basePathUris[i];
            }
            if (scheme != storageScheme)
            {
                throw new PrestoException(PixelsErrorCode.PIXELS_QUERY_PARSING_ERROR,
                        "The storage schemes in 'paths' are inconsistent with 'storage'.");
            }
            if (!basePathUris[i].endsWith("/"))
            {
                basePathUris[i] = basePathUris[i] + "/";
            }
        }
        List<Column> columns = new ArrayList<>();
        for (ColumnMetadata columnMetadata : tableMetadata.getColumns())
        {
            Column column = new Column();
            column.setName(columnMetadata.getName());
            // columnMetadata.getType().getDisplayName(); is the same as
            // columnMetadata.getType().getTypeSignature().toString();
            column.setType(columnMetadata.getType().getDisplayName());
            // column size is set to 0 when the table is just created.
            column.setSize(0);
            columns.add(column);
        }
        try
        {
            boolean res = this.metadataProxy.createTable(schemaName, tableName, storageScheme,
                    Arrays.asList(basePathUris), columns);
            if (!res && !ignoreExisting)
            {
                throw  new PrestoException(PixelsErrorCode.PIXELS_SQL_EXECUTE_ERROR,
                        "Table '" + schemaTableName + "' might already exist, failed to create it.");
            }
        } catch (MetadataException e)
        {
            throw new PrestoException(PixelsErrorCode.PIXELS_METASTORE_ERROR, e);
        }
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        PixelsTableHandle pixelsTableHandle = (PixelsTableHandle) tableHandle;
        String schemaName = pixelsTableHandle.getSchemaName();
        String tableName = pixelsTableHandle.getTableName();

        try
        {
            boolean res = this.metadataProxy.dropTable(schemaName, tableName);
            if (!res)
            {
                throw  new PrestoException(PixelsErrorCode.PIXELS_SQL_EXECUTE_ERROR,
                        "Table " + schemaName + "." + tableName + " does not exist.");
            }
        } catch (MetadataException e)
        {
            throw new PrestoException(PixelsErrorCode.PIXELS_METASTORE_ERROR, e);
        }
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties)
    {
        try
        {
            boolean res = this.metadataProxy.createSchema(schemaName);
            if (res == false)
            {
                throw  new PrestoException(PixelsErrorCode.PIXELS_SQL_EXECUTE_ERROR,
                        "Schema " + schemaName + " already exists.");
            }
        } catch (MetadataException e)
        {
            throw new PrestoException(PixelsErrorCode.PIXELS_METASTORE_ERROR, e);
        }
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName)
    {
        try
        {
            boolean res = this.metadataProxy.dropSchema(schemaName);
            if (res == false)
            {
                throw  new PrestoException(PixelsErrorCode.PIXELS_SQL_EXECUTE_ERROR,
                        "Schema " + schemaName + " does not exist.");
            }
        } catch (MetadataException e)
        {
            throw new PrestoException(PixelsErrorCode.PIXELS_METASTORE_ERROR, e);
        }
    }

    @Override
    public void createView(ConnectorSession session, ConnectorTableMetadata viewMetadata, String viewData, boolean replace)
    {
        try
        {
            SchemaTableName viewName = viewMetadata.getTable();
            boolean res = this.metadataProxy.createView(viewName.getSchemaName(), viewName.getTableName(), viewData);
            if (res == false)
            {
                throw  new PrestoException(PixelsErrorCode.PIXELS_SQL_EXECUTE_ERROR,
                        "Failed to create view '" + viewName + "'.");
            }
        } catch (MetadataException e)
        {
            throw new PrestoException(PixelsErrorCode.PIXELS_METASTORE_ERROR, e);
        }
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        try
        {
            boolean res = this.metadataProxy.dropView(viewName.getSchemaName(), viewName.getTableName());
            if (res == false)
            {
                throw  new PrestoException(PixelsErrorCode.PIXELS_SQL_EXECUTE_ERROR,
                        "View '" + viewName.getSchemaName() + "." + viewName.getTableName() + "' does not exist.");
            }
        } catch (MetadataException e)
        {
            throw new PrestoException(PixelsErrorCode.PIXELS_METASTORE_ERROR, e);
        }
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> schemaName)
    {
        try
        {
            List<String> schemaNames;
            if (schemaName.isPresent())
            {
                schemaNames = ImmutableList.of(schemaName.get());
            } else
            {
                schemaNames = metadataProxy.getSchemaNames();
            }

            ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
            for (String schema : schemaNames)
            {
                /**
                 * PIXELS-194:
                 * Only try to get view names if the schema exists.
                 * 'show tables' in information_schema also invokes this method.
                 * In this case, information_schema does not exist in the metadata,
                 * we should return an empty list without throwing an exception.
                 * Presto will add the system tables by itself.
                 */
                if (metadataProxy.existSchema(schema))
                {
                    for (String table : metadataProxy.getViewNames(schema))
                    {
                        builder.add(new SchemaTableName(schema, table));
                    }
                }
            }
            return builder.build();
        } catch (MetadataException e)
        {
            throw new PrestoException(PixelsErrorCode.PIXELS_METASTORE_ERROR, e);
        }
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, SchemaTablePrefix prefix)
    {
        ImmutableMap.Builder<SchemaTableName, ConnectorViewDefinition> builder = ImmutableMap.builder();
        try
        {
            String schemaName = prefix.getSchemaName();
            if (this.metadataProxy.existSchema(schemaName))
            {
                /**
                 * PIXELS-194:
                 * Only try to get views if the schema exists.
                 * Otherwise, return an empty set, which is required by Presto
                 * when reading the content of information_schema tables.
                 */
                List<View> views = this.metadataProxy.getViews(schemaName);
                for (View view : views)
                {
                    SchemaTableName stName = new SchemaTableName(schemaName, view.getName());
                    builder.put(stName, new ConnectorViewDefinition(stName, Optional.empty(), view.getData()));
                }
            }
            return builder.build();
        } catch (MetadataException e)
        {
            throw new PrestoException(PixelsErrorCode.PIXELS_METASTORE_ERROR, e);
        }
    }
}
