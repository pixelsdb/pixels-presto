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
import io.pixelsdb.pixels.cache.MemoryMappedFile;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.core.PixelsFooterCache;
import io.pixelsdb.pixels.core.lambda.ScanInput;
import io.pixelsdb.pixels.core.lambda.ScanInvoker;
import io.pixelsdb.pixels.core.lambda.ScanOutput;
import io.pixelsdb.pixels.core.predicate.TableScanFilter;
import io.pixelsdb.pixels.presto.exception.PixelsErrorCode;
import io.pixelsdb.pixels.presto.impl.PixelsPrestoConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * Provider Class for Pixels Page Source class.
 */
public class PixelsPageSourceProvider
        implements ConnectorPageSourceProvider
{
    // private static final Logger logger = Logger.get(PixelsPageSourceProvider.class);

    private final String connectorId;
    private final MemoryMappedFile cacheFile;
    private final MemoryMappedFile indexFile;
    private final PixelsFooterCache pixelsFooterCache;
    private final PixelsPrestoConfig config;

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
            if (config.isLambdaEnabled())
            {
                Storage storage = StorageFactory.Instance().getStorage(Storage.Scheme.minio);
                return new PixelsPageSource(pixelsSplit, pixelsColumns, includeCols, storage,
                        cacheFile, indexFile, pixelsFooterCache, getLambdaOutput(pixelsSplit, includeCols));
            }
            else
            {
                Storage storage = StorageFactory.Instance().getStorage(pixelsSplit.getStorageScheme());
                return new PixelsPageSource(pixelsSplit, pixelsColumns, includeCols, storage,
                        cacheFile, indexFile, pixelsFooterCache, null);
            }
        } catch (IOException e)
        {
            throw new PrestoException(PixelsErrorCode.PIXELS_STORAGE_ERROR, e);
        }
    }

    private CompletableFuture<ScanOutput> getLambdaOutput(PixelsSplit inputSplit, String[] includeCols)
    {
        ScanInput scanInput = new ScanInput();
        scanInput.setQueryId(inputSplit.getQueryId());
        List<String> paths = inputSplit.getPaths();
        List<Integer> rgStarts = inputSplit.getRgStarts();
        List<Integer> rgLengths = inputSplit.getRgLengths();
        int splitSize = 0;
        ArrayList<ScanInput.InputInfo> inputInfos = new ArrayList<>(paths.size());
        for (int i = 0; i < paths.size(); ++i)
        {
            inputInfos.add(new ScanInput.InputInfo(paths.get(i), rgStarts.get(i), rgLengths.get(i)));
            splitSize += rgLengths.get(i);
        }
        scanInput.setInputs(inputInfos);
        scanInput.setCols(includeCols);
        scanInput.setSplitSize(splitSize);
        TableScanFilter filter = PixelsSplitManager.createTableScanFilter(inputSplit.getSchemaName(),
                inputSplit.getTableName(), includeCols, inputSplit.getConstraint());
        scanInput.setFilter(JSON.toJSONString(filter));
        String folder = config.getMinioOutputFolder();
        String endpoint = config.getMinioEndpoint();
        String accessKey = config.getMinioAccessKey();
        String secretKey = config.getMinioSecretKey();
        ScanInput.OutputInfo outputInfo = new ScanInput.OutputInfo(folder,
                endpoint, accessKey, secretKey, true);
        scanInput.setOutput(outputInfo);
        return ScanInvoker.invoke(scanInput);
    }
}
