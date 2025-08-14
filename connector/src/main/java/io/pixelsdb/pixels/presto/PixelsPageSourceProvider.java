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

import com.alibaba.fastjson.JSON;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.*;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.inject.Inject;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.physical.natives.MemoryMappedFile;
import io.pixelsdb.pixels.common.turbo.InvokerFactory;
import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.core.PixelsFooterCache;
import io.pixelsdb.pixels.core.utils.Pair;
import io.pixelsdb.pixels.executor.predicate.TableScanFilter;
import io.pixelsdb.pixels.planner.plan.physical.domain.InputSplit;
import io.pixelsdb.pixels.planner.plan.physical.domain.OutputInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.ScanTableInfo;
import io.pixelsdb.pixels.planner.plan.physical.input.ScanInput;
import io.pixelsdb.pixels.planner.plan.physical.output.ScanOutput;
import io.pixelsdb.pixels.presto.exception.PixelsErrorCode;
import io.pixelsdb.pixels.presto.impl.PixelsPrestoConfig;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;
import static io.pixelsdb.pixels.presto.PixelsSplitManager.createTableScanFilter;
import static io.pixelsdb.pixels.presto.PixelsSplitManager.getInputSplit;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * Provider Class for Pixels Page Source class.
 */
public class PixelsPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private static final Logger logger = Logger.get(PixelsPageSourceProvider.class);

    private final String connectorId;
    private final List<MemoryMappedFile> cacheFiles;
    private final List<MemoryMappedFile> indexFiles;
    private final PixelsFooterCache pixelsFooterCache;
    private final PixelsPrestoConfig config;

    private final AtomicInteger localSplitCounter;

    @Inject
    public PixelsPageSourceProvider(PixelsConnectorId connectorId, PixelsPrestoConfig config)
            throws Exception
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.config = requireNonNull(config, "config is null");
        if (config.getConfigFactory().getProperty("cache.enabled").equalsIgnoreCase("true"))
        {
            // NOTICE: creating a MemoryMappedFile is efficient, usually cost tens of us.
            int zoneNum = Integer.parseInt(config.getConfigFactory().getProperty("cache.zone.num"));
            int swapZoneNum = Integer.parseInt(config.getConfigFactory().getProperty("cache.zone.swap.num"));
            long zoneSize = Long.parseLong(config.getConfigFactory().getProperty("cache.size")) / (zoneNum - swapZoneNum);
            long zoneIndexSize = Long.parseLong(config.getConfigFactory().getProperty("index.size")) / (zoneNum - swapZoneNum);
            String zoneLocationPrefix = config.getConfigFactory().getProperty("cache.location");
            String indexLocationPrefix = config.getConfigFactory().getProperty("index.location");
            this.cacheFiles = new java.util.ArrayList<>();
            this.indexFiles = new java.util.ArrayList<>();
            for (int i = 0; i < zoneNum; i++)
            {
                this.cacheFiles.add(new MemoryMappedFile(zoneLocationPrefix + "." + i, zoneSize));
                this.indexFiles.add(new MemoryMappedFile(indexLocationPrefix + "." + i, zoneIndexSize));
            }
            this.indexFiles.add(new MemoryMappedFile(indexLocationPrefix, zoneIndexSize));
        } else
        {
            this.cacheFiles = new java.util.ArrayList<>();
            this.indexFiles = new java.util.ArrayList<>();
        }
        this.pixelsFooterCache = new PixelsFooterCache();
        this.localSplitCounter = new AtomicInteger(0);
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transactionHandle,
                                                ConnectorSession session, ConnectorSplit split,
                                                ConnectorTableLayoutHandle layout,
                                                List<ColumnHandle> columns, SplitContext splitContext)
    {
        List<PixelsColumnHandle> pixelsColumns = columns.stream()
                .map(PixelsColumnHandle.class::cast).collect(toList());
        requireNonNull(split, "split is null");
        PixelsSplit pixelsSplit = (PixelsSplit) split;
        checkArgument(pixelsSplit.getConnectorId().equals(connectorId),
                "connectorId is not for this connector");

        String[] includeCols = new String[pixelsColumns.size()];
        for (int i = 0; i < pixelsColumns.size(); i++)
        {
            includeCols[i] = pixelsColumns.get(i).getColumnName();
        }

        try
        {
            if (config.isCloudFunctionEnabled() && this.localSplitCounter.get() >= config.getLocalScanConcurrency()
                    /**
                     * Issue #12:
                     * If the number of columns to read is 0, the spits should not be processed by Lambda.
                     * It usually means that the query is like select count(*) from table.
                     * Such queries can be served on the metadata headers that are cached locally, without touching the data.
                     */
                    && includeCols.length > 0)
            {
                boolean[] projection = new boolean[includeCols.length];
                Arrays.fill(projection, true);
                Storage storage = StorageFactory.Instance().getStorage(config.getOutputStorageScheme());
                IntermediateFileCleaner.Instance().registerStorage(storage);
                return new PixelsPageSource(pixelsSplit, pixelsColumns, includeCols, storage, cacheFiles, indexFiles,
                        pixelsFooterCache, getLambdaOutput(pixelsSplit, includeCols, projection), null);
            }
            else
            {
                this.localSplitCounter.incrementAndGet();
                Storage storage = StorageFactory.Instance().getStorage(pixelsSplit.getStorageScheme());
                return new PixelsPageSource(pixelsSplit, pixelsColumns, includeCols, storage,
                        cacheFiles, indexFiles, pixelsFooterCache, null, this.localSplitCounter);
            }
        } catch (IOException e)
        {
            throw new PrestoException(PixelsErrorCode.PIXELS_STORAGE_ERROR, e);
        }
    }

    private CompletableFuture<?> getLambdaOutput(PixelsSplit inputSplit, String[] columnsToRead, boolean[] projection)
    {
        checkArgument(Storage.Scheme.from(inputSplit.getStorageScheme()).equals(config.getInputStorageScheme()), String.format(
                "the storage scheme of table '%s.%s' is not consistent with the input storage scheme for Pixels Turbo",
                inputSplit.getSchemaName(), inputSplit.getTableName()));
        ScanInput scanInput = new ScanInput();
        scanInput.setTransId(inputSplit.getTransId());
        scanInput.setOperatorName("scan_" + inputSplit.getTableName());
        ScanTableInfo tableInfo = new ScanTableInfo();
        tableInfo.setTableName(inputSplit.getTableName());
        tableInfo.setColumnsToRead(columnsToRead);
        Pair<Integer, InputSplit> inputSplitPair = getInputSplit(inputSplit);
        tableInfo.setInputSplits(Arrays.asList(inputSplitPair.getRight()));
        TableScanFilter filter = createTableScanFilter(inputSplit.getSchemaName(),
                inputSplit.getTableName(), columnsToRead, inputSplit.getConstraint());
        tableInfo.setFilter(JSON.toJSONString(filter));
        tableInfo.setBase(true);
        tableInfo.setStorageInfo(config.getInputStorageInfo());
        scanInput.setTableInfo(tableInfo);
        scanInput.setScanProjection(projection);
        // logger.info("table scan filter: " + tableInfo.getFilter());
        String folder = config.getOutputFolderForQuery(inputSplit.getTransId()) + inputSplit.getSplitId() + "/";
        OutputInfo outputInfo = new OutputInfo(folder, config.getOutputStorageInfo(), true);
        scanInput.setOutput(outputInfo);

        return InvokerFactory.Instance().getInvoker(WorkerType.SCAN)
                .invoke(scanInput).whenComplete(((scanOutput, err) -> {
            if (err != null)
            {
                throw new RuntimeException("error in lambda invoke.", err);
            }
            try
            {
                inputSplit.permute(config.getOutputStorageScheme(), (ScanOutput) scanOutput);
            }
            catch (Exception e)
            {
                throw new RuntimeException("error in minio read.", e);
            }
        }));
    }
}
