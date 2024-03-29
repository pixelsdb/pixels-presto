/*
 * Copyright 2022 PixelsDB.
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
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.pixelsdb.pixels.cache.PixelsCacheReader;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.natives.MemoryMappedFile;
import io.pixelsdb.pixels.core.PixelsFooterCache;
import io.pixelsdb.pixels.core.PixelsReader;
import io.pixelsdb.pixels.core.PixelsReaderImpl;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.predicate.PixelsPredicate;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.vector.*;
import io.pixelsdb.pixels.presto.exception.PixelsErrorCode;
import io.pixelsdb.pixels.presto.impl.PixelsPrestoConfig;
import io.pixelsdb.pixels.presto.impl.PixelsTupleDomainPredicate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static io.pixelsdb.pixels.core.TypeDescription.Category.*;
import static java.util.Objects.requireNonNull;

/**
 * @date 17/02/2022
 * @author hank
 */
public class PixelsRecordCursor implements RecordCursor
{
    private static final Logger logger = Logger.get(PixelsPageSource.class);
    private final int BatchSize;
    private final PixelsSplit split;
    private final List<PixelsColumnHandle> columns;
    private final String[] includeCols;
    private final Storage storage;
    private boolean closed;
    private PixelsReader pixelsReader;
    private PixelsRecordReader recordReader;
    private final PixelsCacheReader cacheReader;
    private final PixelsFooterCache footerCache;
    private long completedBytes = 0L;
    private long readTimeNanos = 0L;
    private long memoryUsage = 0L;
    private PixelsReaderOption option;
    private final int numColumnToRead;
    /**
     * If rowBatch == null && rowBatchSize > 0, numColumnToRead must be 0.
     * It means that the query is like select count(*) from table, i.e., it
     * does not read any physical data.
     *
     * If rowBatch == null && rowBatchSize == 0, it means that the first row
     * has not been read.
     */
    private VectorizedRowBatch rowBatch;
    private int rowBatchSize;
    private int rowIndex;

    public PixelsRecordCursor(PixelsSplit split, List<PixelsColumnHandle> columnHandles, Storage storage,
                              MemoryMappedFile cacheFile, MemoryMappedFile indexFile, PixelsFooterCache footerCache,
                              String connectorId)
    {
        this.split = split;
        this.storage = storage;
        this.columns = columnHandles;
        this.includeCols = new String[columns.size()];
        for (int i = 0; i < columns.size(); i++)
        {
            this.includeCols[i] = columns.get(i).getColumnName();
        }
        this.numColumnToRead = columnHandles.size();
        this.footerCache = footerCache;
        this.closed = false;
        this.BatchSize = PixelsPrestoConfig.getBatchSize();
        this.rowIndex = -1;
        this.rowBatch = null;
        this.rowBatchSize = 0;

        this.cacheReader = PixelsCacheReader
                .newBuilder()
                .setCacheFile(cacheFile)
                .setIndexFile(indexFile)
                .build();
        readFirstPath(split, cacheReader, footerCache);
    }

    private void readFirstPath(PixelsSplit split, PixelsCacheReader pixelsCacheReader,
                               PixelsFooterCache pixelsFooterCache)
    {
        this.option = new PixelsReaderOption();
        this.option.skipCorruptRecords(true);
        this.option.tolerantSchemaEvolution(true);
        this.option.includeCols(this.includeCols);
        this.option.rgRange(split.getRgStart(), split.getRgLength());
        this.option.transId(split.getTransId());

        if (split.getConstraint().getDomains().isPresent() && !split.getColumnOrder().isEmpty())
        {
            Map<PixelsColumnHandle, Domain> domains = split.getConstraint().getDomains().get();
            List<PixelsTupleDomainPredicate.ColumnReference<PixelsColumnHandle>> columnReferences =
                    new ArrayList<>(domains.size());
            for (Map.Entry<PixelsColumnHandle, Domain> entry : domains.entrySet())
            {
                PixelsColumnHandle column = entry.getKey();
                String columnName = column.getColumnName();
                int columnOrdinal = split.getColumnOrder().indexOf(columnName);
                columnReferences.add(
                        new PixelsTupleDomainPredicate.ColumnReference<>(
                                column,
                                columnOrdinal,
                                column.getColumnType()));
            }
            PixelsPredicate predicate = new PixelsTupleDomainPredicate<>(split.getConstraint(), columnReferences);
            this.option.predicate(predicate);
        }

        try
        {
            if (this.storage != null)
            {
                this.pixelsReader = PixelsReaderImpl
                        .newBuilder()
                        .setStorage(this.storage)
                        .setPath(split.getPath())
                        .setEnableCache(split.getCached())
                        .setCacheOrder(split.getCacheOrder())
                        .setPixelsCacheReader(pixelsCacheReader)
                        .setPixelsFooterCache(pixelsFooterCache)
                        .build();
                if (this.pixelsReader.getRowGroupNum() <= this.option.getRGStart())
                {
                    /**
                     * As PixelsSplitManager does not check the exact number of row groups
                     * in the file, the start row group index might be invalid. in this case,
                     * we can simply close this page source.
                     */
                    this.close();
                } else
                {
                    this.recordReader = this.pixelsReader.read(this.option);
                }
            } else
            {
                logger.error("pixelsReader error: storage handler is null");
                throw new IOException("pixelsReader error: storage handler is null.");
            }
        } catch (IOException e)
        {
            logger.error("pixelsReader error: " + e.getMessage());
            closeWithSuppression(e);
            throw new PrestoException(PixelsErrorCode.PIXELS_READER_ERROR, "create Pixels reader error.", e);
        }
    }

    private boolean readNextPath ()
    {
        try
        {
            if (this.split.nextPath())
            {
                closeReader();
                if (this.storage != null)
                {
                    this.pixelsReader = PixelsReaderImpl
                            .newBuilder()
                            .setStorage(this.storage)
                            .setPath(split.getPath())
                            .setEnableCache(split.getCached())
                            .setCacheOrder(split.getCacheOrder())
                            .setPixelsCacheReader(this.cacheReader)
                            .setPixelsFooterCache(this.footerCache)
                            .build();
                    this.option.rgRange(split.getRgStart(), split.getRgLength());
                    if (this.pixelsReader.getRowGroupNum() <= this.option.getRGStart())
                    {
                        /**
                         * As PixelsSplitManager does not check the exact number of row groups
                         * in the file, the start row group index might be invalid. In this case,
                         * we can simply return false, and the page source will be closed outside.
                         */
                        return false;
                    }
                    this.recordReader = this.pixelsReader.read(this.option);
                } else
                {
                    logger.error("pixelsReader error: storage handler is null");
                    throw new IOException("pixelsReader error: storage handler is null");
                }
                return true;
            } else
            {
                return false;
            }
        } catch (Exception e)
        {
            logger.error("pixelsReader error: " + e.getMessage());
            closeWithSuppression(e);
            throw new PrestoException(PixelsErrorCode.PIXELS_READER_ERROR, "read next path error.", e);
        }
    }

    @Override
    public long getCompletedBytes()
    {
        if (closed)
        {
            return this.completedBytes;
        }
        return this.completedBytes + recordReader.getCompletedBytes();
    }

    @Override
    public long getReadTimeNanos()
    {
        if (closed)
        {
            return readTimeNanos;
        }
        return this.readTimeNanos + recordReader.getReadTimeNanos();
    }

    @Override
    public long getSystemMemoryUsage()
    {
        if (closed)
        {
            return memoryUsage;
        }
        return this.memoryUsage + recordReader.getMemoryUsage();
    }

    @Override
    public Type getType(int field)
    {
        checkArgument(field >= 0 && field < columns.size(), "Invalid field index");
        return this.columns.get(field).getColumnType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (closed)
        {
            return false;
        }

        if (++this.rowIndex < this.rowBatchSize)
        {
            return true;
        }

        if (this.numColumnToRead > 0)
        {
            try
            {
                VectorizedRowBatch newRowBatch = this.recordReader.readBatch(BatchSize, false);
                if (newRowBatch.size <= 0)
                {
                    // reach the end of the file
                    if (readNextPath())
                    {
                        // open and start reading the next file (path).
                        newRowBatch = this.recordReader.readBatch(BatchSize, false);
                    } else
                    {
                        // no more files (paths) to read, close.
                        close();
                        return false;
                    }
                }
                if (this.rowBatch != newRowBatch)
                {
                    // VectorizedRowBatch may be reused by PixelsRecordReader.
                    this.rowBatch = newRowBatch;
                    // this.setColumnVectors();
                }
                this.rowBatchSize = this.rowBatch.size;
                this.rowIndex = -1;
                return advanceNextPosition();
            } catch (IOException e)
            {
                closeWithSuppression(e);
                throw new PrestoException(PixelsErrorCode.PIXELS_BAD_DATA, "read row batch error.", e);
            }
        } else
        {
            // No column to read.
            try
            {
                int size = this.recordReader.prepareBatch(BatchSize);
                if (size <= 0)
                {
                    if (readNextPath())
                    {
                        size = this.recordReader.prepareBatch(BatchSize);
                    } else
                    {
                        close();
                        return false;
                    }
                }
                this.rowBatchSize = size;
                this.rowIndex = -1;
                return advanceNextPosition();
            } catch (IOException e)
            {
                closeWithSuppression(e);
                throw new PrestoException(PixelsErrorCode.PIXELS_BAD_DATA, "prepare row batch error.", e);
            }
        }
    }

    @Override
    public boolean getBoolean(int field)
    {
        return ((ByteColumnVector) this.rowBatch.cols[field]).vector[this.rowIndex] > 0;
    }

    @Override
    public long getLong(int field)
    {
        TypeDescription.Category typeCategory = this.columns.get(field).getTypeCategory();
        switch (typeCategory)
        {
            case INT:
            case LONG:
                return ((LongColumnVector) this.rowBatch.cols[field]).vector[this.rowIndex];
            case DECIMAL:
                /**
                 * PIXELS-196:
                 * Presto call getLong here to get the unscaled value for decimal type.
                 * The precision and scale of decimal are automatically processed by Presto.
                 */
                return ((DecimalColumnVector) this.rowBatch.cols[field]).vector[this.rowIndex];
            case DATE:
                return ((DateColumnVector) this.rowBatch.cols[field]).dates[this.rowIndex];
            case TIME:
                return ((TimeColumnVector) this.rowBatch.cols[field]).times[this.rowIndex];
            case TIMESTAMP:
                /**
                 * PIXELS-297:
                 * times in TimestampColumnVector is in microseconds, whereas in Presto-0.215,
                 * the values of time are expected to be the milliseconds since the epoch.
                 */
                return ((TimestampColumnVector) this.rowBatch.cols[field]).times[this.rowIndex] / 1000;
            default:
                throw new PrestoException(PixelsErrorCode.PIXELS_CURSOR_ERROR,
                        "Column type '" + typeCategory.getPrimaryName() + "' is not Long based.");
        }
    }

    @Override
    public double getDouble(int field)
    {
        return Double.longBitsToDouble(((DoubleColumnVector) this.rowBatch.cols[field]).vector[this.rowIndex]);
    }

    @Override
    public Slice getSlice(int field)
    {
        TypeDescription.Category typeCategory = this.columns.get(field).getTypeCategory();
        checkArgument (typeCategory == VARCHAR || typeCategory == CHAR ||
                        typeCategory == STRING || typeCategory == VARBINARY ||
                        typeCategory == BINARY || typeCategory == DECIMAL,
                "Column type '" + typeCategory.getPrimaryName() + "' is not Slice based.");
        if (typeCategory == DECIMAL)
        {
            // process long decimal.
            // LongDecimalColumnVector columnVector = (LongDecimalColumnVector) this.rowBatch.cols[field];
            // return Slices.wrappedLongArray(columnVector.vector, this.rowIndex*2, 2);
            throw new PrestoException(PixelsErrorCode.PIXELS_DATA_TYPE_ERROR,
                    "long decimal is currently not supported");
        }
        BinaryColumnVector columnVector = (BinaryColumnVector)this.rowBatch.cols[field];
        return Slices.wrappedBuffer(columnVector.vector[this.rowIndex],
                columnVector.start[this.rowIndex], columnVector.lens[this.rowIndex]);
    }

    @Override
    public Object getObject(int field)
    {
        throw new PrestoException(PixelsErrorCode.PIXELS_CURSOR_ERROR,
                "Array or Map type is not supported.");
    }

    @Override
    public boolean isNull(int field)
    {
        if (this.rowBatch == null)
        {
            return this.rowBatchSize > 0;
        }
        checkArgument(field < this.rowBatch.cols.length);
        return this.rowBatch.cols[field].isNull[this.rowIndex];
    }

    @Override
    public void close()
    {
        if (closed)
        {
            return;
        }

        closeReader();
        closed = true;
    }

    private void closeReader()
    {
        try
        {
            if (pixelsReader != null)
            {
                if (recordReader != null)
                {
                    this.completedBytes += recordReader.getCompletedBytes();
                    this.readTimeNanos += recordReader.getReadTimeNanos();
                    this.memoryUsage += recordReader.getMemoryUsage();
                }
                pixelsReader.close();
                recordReader = null;
                pixelsReader = null;
            }
        } catch (Exception e)
        {
            logger.error("close error: " + e.getMessage());
            throw new PrestoException(PixelsErrorCode.PIXELS_READER_CLOSE_ERROR, "close reader error.", e);
        }
    }

    private void closeWithSuppression(Throwable throwable)
    {
        requireNonNull(throwable, "throwable is null");
        try
        {
            close();
        } catch (RuntimeException e)
        {
            // Self-suppression not permitted
            logger.error(e, e.getMessage());
            if (throwable != e)
            {
                throwable.addSuppressed(e);
            }
            throw new PrestoException(PixelsErrorCode.PIXELS_CLIENT_ERROR, "close page source error.", e);
        }
    }
}
