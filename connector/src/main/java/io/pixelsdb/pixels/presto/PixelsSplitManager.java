/*
 * Copyright 2018 PixelsDB.
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
import com.facebook.presto.common.predicate.Marker;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.*;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.etcd.jetcd.KeyValue;
import io.pixelsdb.pixels.cache.PixelsCacheUtil;
import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.layout.*;
import io.pixelsdb.pixels.common.metadata.SchemaTableName;
import io.pixelsdb.pixels.common.metadata.domain.*;
import io.pixelsdb.pixels.common.physical.Location;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.utils.Constants;
import io.pixelsdb.pixels.common.utils.EtcdUtil;
import io.pixelsdb.pixels.core.TypeDescription.Category;
import io.pixelsdb.pixels.core.utils.Pair;
import io.pixelsdb.pixels.executor.predicate.Bound;
import io.pixelsdb.pixels.executor.predicate.ColumnFilter;
import io.pixelsdb.pixels.executor.predicate.Filter;
import io.pixelsdb.pixels.executor.predicate.TableScanFilter;
import io.pixelsdb.pixels.planner.plan.physical.domain.InputInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.InputSplit;
import io.pixelsdb.pixels.presto.exception.CacheException;
import io.pixelsdb.pixels.presto.exception.PixelsErrorCode;
import io.pixelsdb.pixels.presto.impl.PixelsMetadataProxy;
import io.pixelsdb.pixels.presto.impl.PixelsPrestoConfig;
import io.pixelsdb.pixels.presto.properties.PixelsSessionProperties;

import javax.inject.Inject;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static io.pixelsdb.pixels.planner.PixelsPlanner.getFilePaths;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

/**
 * @author hank
 * @author guodong
 * @author tao
 * @create 2018-01-20 19:16
 */
@SuppressWarnings("Duplicates")
public class PixelsSplitManager implements ConnectorSplitManager
{
    private static final Logger logger = Logger.get(PixelsSplitManager.class);
    private final String connectorId;
    private final PixelsMetadataProxy metadataProxy;
    private final PixelsPrestoConfig config;
    private final boolean cacheEnabled;
    private final boolean multiSplitForOrdered;
    private final boolean projectionReadEnabled;
    private final String cacheSchema;
    private final String cacheTable;
    private final int fixedSplitSize;

    @Inject
    public PixelsSplitManager(PixelsConnectorId connectorId, PixelsMetadataProxy metadataProxy, PixelsPrestoConfig config)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.metadataProxy = requireNonNull(metadataProxy, "metadataProxy is null");
        this.config = requireNonNull(config, "config is null");
        String cacheEnabled = config.getConfigFactory().getProperty("cache.enabled");
        String projectionReadEnabled = config.getConfigFactory().getProperty("projection.read.enabled");
        String multiSplit = config.getConfigFactory().getProperty("multi.split.for.ordered");
        this.fixedSplitSize = Integer.parseInt(config.getConfigFactory().getProperty("fixed.split.size"));
        this.cacheEnabled = Boolean.parseBoolean(cacheEnabled);
        this.projectionReadEnabled = Boolean.parseBoolean(projectionReadEnabled);
        this.multiSplitForOrdered = Boolean.parseBoolean(multiSplit);
        KeyValue keyValue = EtcdUtil.Instance().getKeyValue(Constants.LAYOUT_VERSION_LITERAL);
        if (keyValue != null)
        {
            String value = keyValue.getValue().toString(StandardCharsets.UTF_8);
            // PIXELS-636: get schema and table name from etcd instead of config file.
            String[] splits = value.split(":");
            checkArgument(splits.length == 2, "invalid value for key '" +
                    Constants.LAYOUT_VERSION_LITERAL + "' in etcd: " + value);
            SchemaTableName schemaTableName = new SchemaTableName(splits[0]);
            this.cacheSchema = schemaTableName.getSchemaName();
            this.cacheTable = schemaTableName.getTableName();
        }
        else
        {
            this.cacheSchema = null;
            this.cacheTable = null;
        }
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle handle, ConnectorSession session,
                                          ConnectorTableLayoutHandle tableLayout,
                                          SplitSchedulingContext splitSchedulingContext)
    {
        PixelsTransactionHandle transHandle = (PixelsTransactionHandle) handle;
        PixelsTableLayoutHandle layoutHandle = (PixelsTableLayoutHandle) tableLayout;
        PixelsTableHandle tableHandle = layoutHandle.getTable();

        TupleDomain<PixelsColumnHandle> constraint = layoutHandle.getConstraint()
                .transform(PixelsColumnHandle.class::cast);

        String schemaName = tableHandle.getSchemaName();
        String tableName = tableHandle.getTableName();
        Set<PixelsColumnHandle> desiredColumns = layoutHandle.getDesiredColumns().stream().
                map(PixelsColumnHandle.class::cast).collect(toSet());

        Table table;
        Storage storage;
        List<Layout> layouts;
        try
        {
            table = metadataProxy.getTable(transHandle.getTransId(), schemaName, tableName);
            storage = StorageFactory.Instance().getStorage(table.getStorageScheme());
            layouts = metadataProxy.getDataLayouts(schemaName, tableName);
        }
        catch (MetadataException e)
        {
            logger.error(e, "failed to get table or layouts from metadata");
            throw new PrestoException(PixelsErrorCode.PIXELS_METASTORE_ERROR, e);
        } catch (IOException e)
        {
            logger.error(e, "failed to get storage for table");
            throw new PrestoException(PixelsErrorCode.PIXELS_STORAGE_ERROR, e);
        }

        /**
         * PIXELS-78:
         * Only try to use cache for the cached table.
         * By avoiding cache probing for uncached tables, query performance on
         * uncached tables is improved significantly (10%-20%).
         * this.cacheSchema and this.cacheTable are not null if this.cacheEnabled == true.
         */
        boolean usingCache = false;
        if (this.cacheEnabled)
        {
            if (schemaName.equalsIgnoreCase(this.cacheSchema) &&
                    tableName.equalsIgnoreCase(this.cacheTable))
            {
                usingCache = true;
            }
        }

        /**
         * PIXELS-169:
         * We use session properties to configure if the ordered and compact layout paths are enabled.
         */
        boolean orderedPathEnabled = PixelsSessionProperties.getOrderedPathEnabled(session);
        boolean compactPathEnabled = PixelsSessionProperties.getCompactPathEnabled(session);

        List<PixelsSplit> pixelsSplits = new ArrayList<>();
        for (Layout layout : layouts)
        {
            // get index
            long version = layout.getVersion();
            SchemaTableName schemaTableName = new SchemaTableName(schemaName, tableName);
            Ordered order = layout.getOrdered();
            ColumnSet columnSet = new ColumnSet();
            for (PixelsColumnHandle columnHandle : desiredColumns)
            {
                columnSet.addColumn(columnHandle.getColumnName());
            }

            // get split size
            int splitSize;
            Splits splits = layout.getSplits();
            if (this.fixedSplitSize > 0)
            {
                splitSize = this.fixedSplitSize;
            }
            else
            {
                // log.info("columns to be accessed: " + columnSet.toString());
                SplitsIndex splitsIndex = IndexFactory.Instance().getSplitsIndex(schemaTableName);
                try
                {
                    if (splitsIndex == null)
                    {
                        logger.debug("splits index not exist in factory, building index...");
                        splitsIndex = buildSplitsIndex(transHandle.getTransId(), version, order, splits, schemaTableName);
                    } else
                    {
                        long indexVersion = splitsIndex.getVersion();
                        if (indexVersion < version)
                        {
                            logger.debug("splits index version is not up-to-date, updating index...");
                            splitsIndex = buildSplitsIndex(transHandle.getTransId(), version, order, splits, schemaTableName);
                        }
                    }
                } catch (MetadataException e)
                {
                    logger.error(e, "failed to build split index");
                    throw new PrestoException(PixelsErrorCode.PIXELS_METASTORE_ERROR, e);
                }
                SplitPattern bestSplitPattern = splitsIndex.search(columnSet);
                // log.info("bestPattern: " + bestPattern.toString());
                splitSize = bestSplitPattern.getSplitSize();
            }
            logger.debug("using split size: " + splitSize);
            int rowGroupNum = splits.getNumRowGroupInFile();

            // get compact paths
            List<Path> compactPaths;
            if (projectionReadEnabled)
            {
                ProjectionsIndex projectionsIndex = IndexFactory.Instance().getProjectionsIndex(schemaTableName);
                Projections projections = layout.getProjections();
                if (projectionsIndex == null)
                {
                    logger.debug("projections index not exist in factory, building index...");
                    projectionsIndex = buildProjectionsIndex(order, projections, schemaTableName);
                }
                else
                {
                    int indexVersion = projectionsIndex.getVersion();
                    if (indexVersion < version)
                    {
                        logger.debug("projections index is not up-to-date, updating index...");
                        projectionsIndex = buildProjectionsIndex(order, projections, schemaTableName);
                    }
                }
                ProjectionPattern projectionPattern = projectionsIndex.search(columnSet);
                if (projectionPattern != null)
                {
                    logger.debug("suitable projection pattern is found");
                    long[] projectionPathIds = projectionPattern.getPathIds();
                    Map<Long, Path> projectionPaths = layout.getProjectionPaths();
                    compactPaths = new ArrayList<>(projectionPathIds.length);
                    for (long projectionPathId : projectionPathIds)
                    {
                        compactPaths.add(projectionPaths.get(projectionPathId));
                    }
                }
                else
                {
                    compactPaths = layout.getCompactPaths();
                }
            }
            else
            {
                compactPaths = layout.getCompactPaths();
            }
            logger.debug("using compact paths: " + compactPaths);

            long splitId = 0;
            if(usingCache)
            {
                Compact compact = layout.getCompact();
                int cacheBorder = compact.getCacheBorder();
                List<String> cacheColumnChunkOrders = compact.getColumnChunkOrder().subList(0, cacheBorder);
                String cacheVersion;
                EtcdUtil etcdUtil = EtcdUtil.Instance();
                KeyValue keyValue = etcdUtil.getKeyValue(Constants.CACHE_VERSION_LITERAL);
                if(keyValue != null)
                {
                    // 1. get version
                    String value = keyValue.getValue().toString(StandardCharsets.UTF_8);
                    String[] valueSplits = value.split(":");
                    checkArgument(valueSplits.length == 2, "invalid value for key '" +
                            Constants.CACHE_VERSION_LITERAL + "' in etcd: " + value);
                    cacheVersion = valueSplits[1];
                    logger.debug("cache version: " + cacheVersion);
                    // 2. get the cached files of each node
                    List<KeyValue> nodeFiles = etcdUtil.getKeyValuesByPrefix(
                            Constants.CACHE_LOCATION_LITERAL + cacheVersion);
                    if(!nodeFiles.isEmpty())
                    {
                        Map<String, String> fileToNodeMap = new HashMap<>();
                        for (KeyValue kv : nodeFiles)
                        {
                            String node = PixelsCacheUtil.getHostnameFromCacheLocationLiteral(
                                    kv.getKey().toString(StandardCharsets.UTF_8));
                            String[] files = kv.getValue().toString(StandardCharsets.UTF_8).split(";");
                            for(String file : files)
                            {
                                fileToNodeMap.put(file, node);
                                // log.info("cache location: {file='" + file + "', node='" + node + "'");
                            }
                        }
                        try
                        {
                            // 3. add splits in orderedPaths
                            if (orderedPathEnabled)
                            {
                                List<String> orderedFilePaths = getFilePaths(
                                        layout.getOrderedPaths(), metadataProxy.getMetadataService());

                                int numPath = orderedFilePaths.size();
                                for (int i = 0; i < numPath;)
                                {
                                    int firstPath = i; // the path of the first ordered file in the split.
                                    List<String> paths = new ArrayList<>(this.multiSplitForOrdered ? splitSize : 1);
                                    if (this.multiSplitForOrdered)
                                    {
                                        for (int j = 0; j < splitSize && i < numPath; ++j, ++i)
                                        {
                                            paths.add(orderedFilePaths.get(i));
                                        }
                                    } else
                                    {
                                        paths.add(orderedFilePaths.get(i++));
                                    }

                                    // We do not cache files in the ordered paths, thus get locations from the storage.
                                    List<HostAddress> orderedAddresses = toHostAddresses(
                                            storage.getLocations(orderedFilePaths.get(firstPath)));

                                    PixelsSplit pixelsSplit = new PixelsSplit(
                                            transHandle.getTransId(), splitId++, connectorId,
                                            tableHandle.getSchemaName(), tableHandle.getTableName(),
                                            table.getStorageScheme().name(), paths,
                                            Collections.nCopies(paths.size(), 0),
                                            Collections.nCopies(paths.size(), 1),
                                            false, storage.hasLocality(), orderedAddresses,
                                            order.getColumnOrder(), new ArrayList<>(0), constraint);
                                    // log.debug("Split in orderPaths: " + pixelsSplit.toString());
                                    pixelsSplits.add(pixelsSplit);
                                }
                            }
                            // 4. add splits in compactPaths
                            if (compactPathEnabled)
                            {
                                int curFileRGIdx;
                                List<String> compactFilePaths = getFilePaths(
                                        compactPaths, metadataProxy.getMetadataService());
                                for (String path : compactFilePaths)
                                {
                                    curFileRGIdx = 0;
                                    while (curFileRGIdx < rowGroupNum)
                                    {
                                        String node = fileToNodeMap.get(path);
                                        List<HostAddress> compactAddresses;
                                        boolean ensureLocality;
                                        if (node == null)
                                        {
                                            // this file is not cached, get the locations from the storage.
                                            compactAddresses = toHostAddresses(storage.getLocations(path));
                                            ensureLocality = storage.hasLocality();
                                        } else
                                        {
                                            // this file is cached.
                                            ImmutableList.Builder<HostAddress> builder = ImmutableList.builder();
                                            builder.add(HostAddress.fromString(node));
                                            compactAddresses = builder.build();
                                            ensureLocality = true;
                                        }

                                        PixelsSplit pixelsSplit = new PixelsSplit(
                                                transHandle.getTransId(), splitId++, connectorId,
                                                tableHandle.getSchemaName(), tableHandle.getTableName(),
                                                table.getStorageScheme().name(), Arrays.asList(path),
                                                Arrays.asList(curFileRGIdx), Arrays.asList(splitSize),
                                                true, ensureLocality, compactAddresses, order.getColumnOrder(),
                                                cacheColumnChunkOrders, constraint);
                                        pixelsSplits.add(pixelsSplit);
                                        // log.debug("Split in compactPaths" + pixelsSplit.toString());
                                        curFileRGIdx += splitSize;
                                    }
                                }
                            }
                        }
                        catch (Exception e)
                        {
                            throw new PrestoException(PixelsErrorCode.PIXELS_STORAGE_ERROR, e);
                        }
                    }
                    else
                    {
                        logger.error("Get caching files error when version is " + cacheVersion);
                        throw new PrestoException(PixelsErrorCode.PIXELS_CACHE_NODE_FILE_ERROR,
                                new CacheException("Get caching files error when version is " + cacheVersion));
                    }
                }
                else
                {
                    throw new PrestoException(PixelsErrorCode.PIXELS_CACHE_VERSION_ERROR,
                            new CacheException("Failed to get cache version from etcd"));
                }
            }
            else
            {
                logger.debug("cache is disabled or no cache available on this table");
                try
                {
                    // 1. add splits in orderedPaths
                    if (orderedPathEnabled)
                    {
                        List<String> orderedFilePaths = getFilePaths(
                                layout.getOrderedPaths(), metadataProxy.getMetadataService());

                        int numPath = orderedFilePaths.size();
                        for (int i = 0; i < numPath;)
                        {
                            int firstPath = i;
                            List<String> paths = new ArrayList<>(this.multiSplitForOrdered ? splitSize : 1);
                            if (this.multiSplitForOrdered)
                            {
                                for (int j = 0; j < splitSize && i < numPath; ++j, ++i)
                                {
                                    paths.add(orderedFilePaths.get(i));
                                }
                            } else
                            {
                                paths.add(orderedFilePaths.get(i++));
                            }

                            List<HostAddress> orderedAddresses = toHostAddresses(
                                    storage.getLocations(orderedFilePaths.get(firstPath)));

                            PixelsSplit pixelsSplit = new PixelsSplit(
                                    transHandle.getTransId(), splitId++, connectorId,
                                    tableHandle.getSchemaName(), tableHandle.getTableName(),
                                    table.getStorageScheme().name(), paths,
                                    Collections.nCopies(paths.size(), 0),
                                    Collections.nCopies(paths.size(), 1),
                                    false, storage.hasLocality(), orderedAddresses,
                                    order.getColumnOrder(), new ArrayList<>(0), constraint);
                            // logger.debug("Split in orderPaths: " + pixelsSplit.toString());
                            pixelsSplits.add(pixelsSplit);
                        }
                    }
                    // 2. add splits in compactPaths
                    if (compactPathEnabled)
                    {
                        List<String> compactFilePaths = getFilePaths(
                                compactPaths, metadataProxy.getMetadataService());

                        int curFileRGIdx;
                        for (String path : compactFilePaths)
                        {
                            curFileRGIdx = 0;
                            while (curFileRGIdx < rowGroupNum)
                            {
                                List<HostAddress> compactAddresses = toHostAddresses(storage.getLocations(path));

                                PixelsSplit pixelsSplit = new PixelsSplit(
                                        transHandle.getTransId(), splitId++, connectorId,
                                        tableHandle.getSchemaName(), tableHandle.getTableName(),
                                        table.getStorageScheme().name(), Arrays.asList(path),
                                        Arrays.asList(curFileRGIdx), Arrays.asList(splitSize),
                                        false, storage.hasLocality(), compactAddresses,
                                        order.getColumnOrder(), new ArrayList<>(0), constraint);
                                pixelsSplits.add(pixelsSplit);
                                curFileRGIdx += splitSize;
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    throw new PrestoException(PixelsErrorCode.PIXELS_STORAGE_ERROR, e);
                }
            }
        }

        Collections.shuffle(pixelsSplits);

        return new PixelsSplitSource(pixelsSplits);
    }

    public static Pair<Integer, InputSplit> getInputSplit(PixelsSplit split)
    {
        ArrayList<InputInfo> inputInfos = new ArrayList<>();
        int splitSize = 0;
        List<String> paths = split.getPaths();
        List<Integer> rgStarts = split.getRgStarts();
        List<Integer> rgLengths = split.getRgLengths();

        for (int i = 0; i < paths.size(); ++i)
        {
            inputInfos.add(new InputInfo(paths.get(i), rgStarts.get(i), rgLengths.get(i)));
            splitSize += rgLengths.get(i);
        }
        return new Pair<>(splitSize, new InputSplit(inputInfos));
    }

    public static TableScanFilter createTableScanFilter(
            String schemaName, String tableName,
            String[] includeCols, TupleDomain<PixelsColumnHandle> constraint)
    {
        SortedMap<Integer, ColumnFilter> columnFilters = new TreeMap<>();
        TableScanFilter tableScanFilter = new TableScanFilter(schemaName, tableName, columnFilters);
        Map<String, Integer> colToCid = new HashMap<>(includeCols.length);
        for (int i = 0; i < includeCols.length; ++i)
        {
            colToCid.put(includeCols[i], i);
        }
        if (constraint.getColumnDomains().isPresent())
        {
            List<TupleDomain.ColumnDomain<PixelsColumnHandle>> columnDomains = constraint.getColumnDomains().get();
            for (TupleDomain.ColumnDomain<PixelsColumnHandle> columnDomain : columnDomains)
            {
                ColumnFilter<?> columnFilter = createColumnFilter(columnDomain);
                columnFilters.put(colToCid.get(columnDomain.getColumn().getColumnName()), columnFilter);
            }
        }

        return tableScanFilter;
    }

    private static  <T extends Comparable<T>> ColumnFilter<T> createColumnFilter(
            TupleDomain.ColumnDomain<PixelsColumnHandle> columnDomain)
    {
        Type prestoType = columnDomain.getDomain().getType();
        String columnName = columnDomain.getColumn().getColumnName();
        Category columnType = columnDomain.getColumn().getTypeCategory();
        Class<?> filterJavaType = columnType.getInternalJavaType() == byte[].class ?
                String.class : columnType.getInternalJavaType();
        boolean isAll = columnDomain.getDomain().isAll();
        boolean isNone = columnDomain.getDomain().isNone();
        boolean allowNull = columnDomain.getDomain().isNullAllowed();
        boolean onlyNull = columnDomain.getDomain().isOnlyNull();

        Filter<T> filter = columnDomain.getDomain().getValues().getValuesProcessor().transform(
                ranges -> {
                    Filter<T> res = new Filter<>(filterJavaType, isAll, isNone, allowNull, onlyNull);
                    if (ranges.getRangeCount() > 0)
                    {
                        ranges.getOrderedRanges().forEach(range ->
                        {
                            if (range.isSingleValue())
                            {
                                Bound<?> bound = createBound(prestoType, Bound.Type.INCLUDED,
                                        range.getLow().getValue());
                                res.addDiscreteValue((Bound<T>) bound);
                            } else
                            {
                                Bound.Type lowerBoundType = range.getLow().getBound() ==
                                        Marker.Bound.EXACTLY ? Bound.Type.INCLUDED : Bound.Type.EXCLUDED;
                                Bound.Type upperBoundType = range.getHigh().getBound() ==
                                        Marker.Bound.EXACTLY ? Bound.Type.INCLUDED : Bound.Type.EXCLUDED;
                                Object lowerBoundValue = null, upperBoundValue = null;
                                if (range.getLow().isLowerUnbounded())
                                {
                                    lowerBoundType = Bound.Type.UNBOUNDED;
                                } else
                                {
                                    lowerBoundValue = range.getLow().getValue();
                                }
                                if (range.getHigh().isUpperUnbounded())
                                {
                                    upperBoundType = Bound.Type.UNBOUNDED;
                                } else
                                {
                                    upperBoundValue = range.getHigh().getValue();
                                }
                                Bound<?> lowerBound = createBound(prestoType, lowerBoundType, lowerBoundValue);
                                Bound<?> upperBound = createBound(prestoType, upperBoundType, upperBoundValue);
                                res.addRange((Bound<T>) lowerBound, (Bound<T>) upperBound);
                            }
                        });
                    }
                    return res;
                },
                discreteValues -> {
                    Filter<T> res = new Filter<>(filterJavaType, isAll, isNone, allowNull, onlyNull);
                    Bound.Type boundType = discreteValues.isWhiteList() ?
                            Bound.Type.INCLUDED : Bound.Type.EXCLUDED;
                    discreteValues.getValues().forEach(value ->
                    {
                        if (value == null)
                        {
                            throw new PrestoException(PixelsErrorCode.PIXELS_INVALID_METADATA,
                                    "discrete value is null");
                        } else
                        {
                            Bound<?> bound = createBound(prestoType, boundType, value);
                            res.addDiscreteValue((Bound<T>) bound);
                        }
                    });
                    return res;
                },
                allOrNone -> new Filter<>(filterJavaType, isAll, isNone, allowNull, onlyNull)
        );
        return new ColumnFilter<>(columnName, columnType, filter);
    }

    private static Bound<?> createBound(Type prestoType, Bound.Type boundType, Object value)
    {
        Class<?> javaType = prestoType.getJavaType();
        Bound<?> bound = null;
        if (boundType == Bound.Type.UNBOUNDED)
        {
            bound = new Bound<>(boundType, null);
        }
        else
        {
            requireNonNull(value, "the value of the bound is null");
            if (javaType == long.class)
            {
                switch (prestoType.getTypeSignature().getBase())
                {
                    case StandardTypes.DATE:
                    case StandardTypes.TIME:
                        bound = new Bound<>(boundType, ((Long) value).intValue());
                        break;
                    default:
                        bound = new Bound<>(boundType, (Long) value);
                        break;
                }
            }
            else if (javaType == int.class)
            {
                bound = new Bound<>(boundType, (Integer) value);
            }
            else if (javaType == double.class)
            {
                bound = new Bound<>(boundType, Double.doubleToLongBits((Double) value));
            }
            else if (javaType == boolean.class)
            {
                bound = new Bound<>(boundType, (byte) ((Boolean) value ? 1 : 0));
            }
            else if (javaType == Slice.class)
            {
                bound = new Bound<>(boundType, ((Slice) value).toString(StandardCharsets.UTF_8).trim());
            }
            else
            {
                throw new PrestoException(PixelsErrorCode.PIXELS_DATA_TYPE_ERROR,
                        "unsupported data type for filter bound: " + javaType.getName());
            }
        }
        return bound;
    }

    private List<HostAddress> toHostAddresses(List<Location> locations)
    {
        ImmutableList.Builder<HostAddress> addressBuilder = ImmutableList.builder();
        for (Location location : locations)
        {
            for (String host : location.getHosts())
            {
                addressBuilder.add(HostAddress.fromString(host));
            }
        }
        return addressBuilder.build();
    }

    private SplitsIndex buildSplitsIndex(long transId, long version, Ordered ordered,
                                         Splits splits, SchemaTableName schemaTableName) throws MetadataException
    {
        List<String> columnOrder = ordered.getColumnOrder();
        SplitsIndex index;
        String indexTypeName = config.getConfigFactory().getProperty("splits.index.type");
        SplitsIndex.IndexType indexType = SplitsIndex.IndexType.valueOf(indexTypeName.toUpperCase());
        switch (indexType)
        {
            case INVERTED:
                index = new InvertedSplitsIndex(version, columnOrder, SplitPattern.buildPatterns(columnOrder, splits),
                        splits.getNumRowGroupInFile());
                break;
            case COST_BASED:
                index = new CostBasedSplitsIndex(transId, version, this.metadataProxy.getMetadataService(),
                        schemaTableName, splits.getNumRowGroupInFile(), splits.getNumRowGroupInFile());
                break;
            default:
                throw new UnsupportedOperationException("splits index type '" + indexType + "' is not supported");
        }
        IndexFactory.Instance().cacheSplitsIndex(schemaTableName, index);
        return index;
    }

    private ProjectionsIndex buildProjectionsIndex(Ordered ordered, Projections projections, SchemaTableName schemaTableName)
    {
        List<String> columnOrder = ordered.getColumnOrder();
        ProjectionsIndex index;
        index = new InvertedProjectionsIndex(columnOrder, ProjectionPattern.buildPatterns(columnOrder, projections));
        IndexFactory.Instance().cacheProjectionsIndex(schemaTableName, index);
        return index;
    }
}