/*
 * Copyright 2021 PixelsDB.
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
package io.pixelsdb.pixels.presto.block;

import com.facebook.presto.spi.block.*;
import org.openjdk.jol.info.ClassLayout;

import java.util.function.BiConsumer;

import static io.pixelsdb.pixels.presto.block.BlockUtil.*;
import static io.airlift.slice.SizeOf.sizeOf;

/**
 * This class is derived from com.facebook.presto.spi.block.IntArrayBlock.
 *
 * With this class, we use int values to simulate a LongArrayBlock, so that
 * we can reduce 50% memory footprint. Int value is enough for time type
 * in Pixels.
 *
 * Modifications:
 * 1. add getLong, getShort, getByte, so that this class can be compatible
 * with com.facebook.presto.spi.block.LongArrayBlock.
 *
 * 2. change the returned statement of the methods that return Block or
 * BlockEncoding.
 *
 * @date 26/04/2021
 * @author hank
 */
public class TimeArrayBlock implements Block
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(TimeArrayBlock.class).instanceSize();

    private final int arrayOffset;
    private final int positionCount;
    private final boolean[] valueIsNull;
    private final int[] values;

    private final long sizeInBytes;
    private final long retainedSizeInBytes;

    public TimeArrayBlock(int positionCount, boolean[] valueIsNull, int[] values)
    {
        this(0, positionCount, valueIsNull, values);
    }

    TimeArrayBlock(int arrayOffset, int positionCount, boolean[] valueIsNull, int[] values)
    {
        if (arrayOffset < 0) {
            throw new IllegalArgumentException("arrayOffset is negative");
        }
        this.arrayOffset = arrayOffset;
        if (positionCount < 0) {
            throw new IllegalArgumentException("positionCount is negative");
        }
        this.positionCount = positionCount;

        if (values.length - arrayOffset < positionCount) {
            throw new IllegalArgumentException("values length is less than positionCount");
        }
        this.values = values;

        if (valueIsNull.length - arrayOffset < positionCount) {
            throw new IllegalArgumentException("isNull length is less than positionCount");
        }
        this.valueIsNull = valueIsNull;

        sizeInBytes = (Integer.BYTES + Byte.BYTES) * (long) positionCount;
        retainedSizeInBytes = INSTANCE_SIZE + sizeOf(valueIsNull) + sizeOf(values);
    }

    @Override
    public long getSizeInBytes()
    {
        return sizeInBytes;
    }

    @Override
    public long getRegionSizeInBytes(int position, int length)
    {
        return (Integer.BYTES + Byte.BYTES) * (long) length;
    }

    /**
     * Returns the size of of all positions marked true in the positions array.
     * This is equivalent to multiple calls of {@code block.getRegionSizeInBytes(position, length)}
     * where you mark all positions for the regions first.
     *
     * @param positions
     */
    @Override
    public long getPositionsSizeInBytes(boolean[] positions)
    {
        int usedPositionCount = 0;
        for (boolean marked : positions)
        {
            if (marked)
            {
                usedPositionCount++;
            }
        }
        return (Integer.BYTES + Byte.BYTES) * (long) usedPositionCount;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return retainedSizeInBytes;
    }

    /**
     * Returns the estimated in memory data size for stats of position.
     * Do not use it for other purpose.
     *
     * @param position
     */
    @Override
    public long getEstimatedDataSizeForStats(int position)
    {
        return isNull(position) ? 0 : Integer.BYTES;
    }

    @Override
    public void retainedBytesForEachPart(BiConsumer<Object, Long> consumer)
    {
        consumer.accept(values, sizeOf(values));
        consumer.accept(valueIsNull, sizeOf(valueIsNull));
        consumer.accept(this, (long) INSTANCE_SIZE);
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public long getLong(int position, int offset)
    {
        checkReadablePosition(position);
        if (offset != 0) {
            throw new IllegalArgumentException("offset must be zero");
        }
        return values[position + arrayOffset];
    }

    @Override
    public int getInt(int position, int offset)
    {
        checkReadablePosition(position);
        if (offset != 0) {
            throw new IllegalArgumentException("offset must be zero");
        }
        return values[position + arrayOffset];
    }

    @Override
    @Deprecated
    // TODO: Remove when we fix intermediate types on aggregations.
    public short getShort(int position, int offset)
    {
        checkReadablePosition(position);
        if (offset != 0) {
            throw new IllegalArgumentException("offset must be zero");
        }

        short value = (short) (values[position + arrayOffset]);
        if (value != values[position + arrayOffset]) {
            throw new ArithmeticException("short overflow");
        }
        return value;
    }

    @Override
    @Deprecated
    // TODO: Remove when we fix intermediate types on aggregations.
    public byte getByte(int position, int offset)
    {
        checkReadablePosition(position);
        if (offset != 0) {
            throw new IllegalArgumentException("offset must be zero");
        }

        byte value = (byte) (values[position + arrayOffset]);
        if (value != values[position + arrayOffset]) {
            throw new ArithmeticException("byte overflow");
        }
        return value;
    }

    @Override
    public boolean isNull(int position)
    {
        checkReadablePosition(position);
        return valueIsNull[position + arrayOffset];
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder)
    {
        checkReadablePosition(position);
        blockBuilder.writeInt(values[position + arrayOffset]);
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        checkReadablePosition(position);
        return new TimeArrayBlock(
                1,
                new boolean[] {valueIsNull[position + arrayOffset]},
                new int[] {values[position + arrayOffset]});
    }

    @Override
    public Block copyPositions(int[] positions, int offset, int length)
    {
        checkArrayRange(positions, offset, length);

        boolean[] newValueIsNull = new boolean[length];
        int[] newValues = new int[length];
        for (int i = 0; i < length; i++) {
            int position = positions[offset + i];
            checkReadablePosition(position);
            newValueIsNull[i] = valueIsNull[position + arrayOffset];
            newValues[i] = values[position + arrayOffset];
        }
        return new TimeArrayBlock(length, newValueIsNull, newValues);
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        checkValidRegion(getPositionCount(), positionOffset, length);

        return new TimeArrayBlock(positionOffset + arrayOffset, length, valueIsNull, values);
    }

    @Override
    public Block copyRegion(int positionOffset, int length)
    {
        checkValidRegion(getPositionCount(), positionOffset, length);

        positionOffset += arrayOffset;
        boolean[] newValueIsNull = compactArray(valueIsNull, positionOffset, length);
        int[] newValues = compactArray(values, positionOffset, length);

        if (newValueIsNull == valueIsNull && newValues == values) {
            return this;
        }
        return new TimeArrayBlock(length, newValueIsNull, newValues);
    }

    @Override
    public String getEncodingName()
    {
        return TimeArrayBlockEncoding.NAME;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("TimeArrayBlock{");
        sb.append("positionCount=").append(getPositionCount());
        sb.append('}');
        return sb.toString();
    }

    private void checkReadablePosition(int position)
    {
        if (position < 0 || position >= getPositionCount()) {
            throw new IllegalArgumentException("position is not valid");
        }
    }
}
