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
import com.facebook.presto.spi.*;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.pixelsdb.pixels.cache.MemoryMappedFile;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.physical.storage.MinIO;
import io.pixelsdb.pixels.core.PixelsFooterCache;
import io.pixelsdb.pixels.core.utils.Pair;
import io.pixelsdb.pixels.executor.lambda.InvokerFactory;
import io.pixelsdb.pixels.executor.lambda.WorkerType;
import io.pixelsdb.pixels.executor.lambda.domain.InputSplit;
import io.pixelsdb.pixels.executor.lambda.domain.OutputInfo;
import io.pixelsdb.pixels.executor.lambda.domain.ScanTableInfo;
import io.pixelsdb.pixels.executor.lambda.domain.StorageInfo;
import io.pixelsdb.pixels.executor.lambda.input.ScanInput;
import io.pixelsdb.pixels.executor.lambda.output.ScanOutput;
import io.pixelsdb.pixels.executor.predicate.TableScanFilter;
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
    private final MemoryMappedFile cacheFile;
    private final MemoryMappedFile indexFile;
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
            this.cacheFile = new MemoryMappedFile(
                    config.getConfigFactory().getProperty("cache.location"),
                    Long.parseLong(config.getConfigFactory().getProperty("cache.size")));
            this.indexFile = new MemoryMappedFile(
                    config.getConfigFactory().getProperty("index.location"),
                    Long.parseLong(config.getConfigFactory().getProperty("index.size")));
        } else
        {
            this.cacheFile = null;
            this.indexFile = null;
        }
        this.pixelsFooterCache = new PixelsFooterCache();
        this.localSplitCounter = new AtomicInteger(0);
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transactionHandle,
                                                ConnectorSession session, ConnectorSplit split,
                                                List<ColumnHandle> columns)
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
            if (config.isLambdaEnabled() && this.localSplitCounter.get() >= config.getLocalScanConcurrency())
            {
                MinIO.ConfigMinIO(config.getMinioEndpoint(), config.getMinioAccessKey(), config.getMinioSecretKey());
                Storage storage = StorageFactory.Instance().getStorage(Storage.Scheme.minio);
                IntermediateFileCleaner.Instance().registerStorage(storage);
                return new PixelsPageSource(pixelsSplit, pixelsColumns, includeCols, storage, cacheFile, indexFile,
                        pixelsFooterCache, getLambdaOutput(pixelsSplit, includeCols), null);
            }
            else
            {
                this.localSplitCounter.incrementAndGet();
                Storage storage = StorageFactory.Instance().getStorage(pixelsSplit.getStorageScheme());
                return new PixelsPageSource(pixelsSplit, pixelsColumns, includeCols, storage,
                        cacheFile, indexFile, pixelsFooterCache, null, this.localSplitCounter);
            }
        } catch (IOException e)
        {
            throw new PrestoException(PixelsErrorCode.PIXELS_STORAGE_ERROR, e);
        }
    }

    private CompletableFuture<?> getLambdaOutput(PixelsSplit inputSplit, String[] includeCols)
    {
        ScanInput scanInput = new ScanInput();
        scanInput.setQueryId(inputSplit.getQueryId());
        ScanTableInfo tableInfo = new ScanTableInfo();
        tableInfo.setTableName(inputSplit.getTableName());
        tableInfo.setColumnsToRead(includeCols);
        Pair<Integer, InputSplit> inputSplitPair = getInputSplit(inputSplit);
        tableInfo.setInputSplits(Arrays.asList(inputSplitPair.getRight()));
        TableScanFilter filter = createTableScanFilter(inputSplit.getSchemaName(),
                inputSplit.getTableName(), includeCols, inputSplit.getConstraint());
        tableInfo.setFilter(JSON.toJSONString(filter));
        scanInput.setTableInfo(tableInfo);
        // logger.info("table scan filter: " + tableInfo.getFilter());
        String folder = config.getMinioOutputFolderForQuery(inputSplit.getQueryId());
        String endpoint = config.getMinioEndpoint();
        String accessKey = config.getMinioAccessKey();
        String secretKey = config.getMinioSecretKey();
        OutputInfo outputInfo = new OutputInfo(folder, true,
                new StorageInfo(Storage.Scheme.minio, endpoint, accessKey, secretKey), true);
        scanInput.setOutput(outputInfo);

        return InvokerFactory.Instance().getInvoker(WorkerType.SCAN)
                .invoke(scanInput).whenComplete(((scanOutput, err) -> {
            if (err != null)
            {
                throw new RuntimeException("error in lambda invoke.", err);
            }
            try
            {
                inputSplit.permute(Storage.Scheme.minio, (ScanOutput) scanOutput);
            }
            catch (Exception e)
            {
                throw new RuntimeException("error in minio read.", e);
            }
        }));
    }
}
