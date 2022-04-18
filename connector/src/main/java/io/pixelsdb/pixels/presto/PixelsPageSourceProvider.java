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

import com.facebook.presto.spi.*;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.pixelsdb.pixels.cache.MemoryMappedFile;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.core.PixelsFooterCache;
import io.pixelsdb.pixels.presto.exception.PixelsErrorCode;
import io.pixelsdb.pixels.presto.impl.PixelsPrestoConfig;

import java.io.IOException;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
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

    @Inject
    public PixelsPageSourceProvider(PixelsConnectorId connectorId, PixelsPrestoConfig config)
            throws Exception
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
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
        try
        {
            Storage storage = StorageFactory.Instance().getStorage(pixelsSplit.getStorageScheme());
            return new PixelsPageSource(pixelsSplit, pixelsColumns, storage,
                    cacheFile, indexFile, pixelsFooterCache, connectorId);
        } catch (IOException e)
        {
            throw new PrestoException(PixelsErrorCode.PIXELS_STORAGE_ERROR, e);
        }
    }

    public static class PageSourceQueue
    {

    }
}
