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
package io.pixelsdb.pixels.presto.block;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockEncoding;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;

import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.pixelsdb.pixels.presto.block.EncoderUtil.decodeNullBits;
import static io.pixelsdb.pixels.presto.block.EncoderUtil.encodeNullsAsBits;

/**
 * This class is derived from com.facebook.presto.spi.block.VariableWidthBlockEncoding
 *
 * We reimplemented writeBlock and readBlock
 *
 * @date 19-6-1
 * @author hank
 */
public class VarcharArrayBlockEncoding implements BlockEncoding
{
    public static final String NAME = "VARCHAR_ARRAY";

    private static final VarcharArrayBlockEncoding instance = new VarcharArrayBlockEncoding();

    public static VarcharArrayBlockEncoding Instance()
    {
        return instance;
    }

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public void writeBlock(BlockEncodingSerde blockEncodingSerde, SliceOutput sliceOutput, Block block)
    {
        // The down casts here are safe because it is the block itself the provides this encoding implementation.
        VarcharArrayBlock varcharArrayBlock = (VarcharArrayBlock) block;

        int positionCount = varcharArrayBlock.getPositionCount();
        sliceOutput.appendInt(positionCount);

        // do not encode offsets, they should be 0.

        // lengths
        for (int position = 0; position < positionCount; position++)
        {
            sliceOutput.appendInt(varcharArrayBlock.getSliceLength(position));
        }

        // isNull
        encodeNullsAsBits(sliceOutput, varcharArrayBlock);

        // values
        // sliceOutput.appendInt((int) varcharArrayBlock.getSizeInBytes());
        for (int position = 0; position < positionCount; position++)
        {
            sliceOutput.writeBytes(varcharArrayBlock.getRawValue(position), varcharArrayBlock.getPositionOffset(position),
                    varcharArrayBlock.getSliceLength(position));
        }
    }

    @Override
    public Block readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput)
    {
        int positionCount = sliceInput.readInt();

        int[] offsets = new int[positionCount];
        int[] lengths = new int[positionCount];

        // offsets should be 0, do not read them from sliceInput.

        // destinationIndex should be 0, because we do not need 0 the be the first item in lengths.
        sliceInput.readBytes(Slices.wrappedIntArray(lengths), 0, positionCount * SIZE_OF_INT);

        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount);

        // int blockSize = sliceInput.readInt();
        byte[][] values = new byte[positionCount][];
        for (int position = 0; position < positionCount; position++)
        {
            values[position] = new byte[lengths[position]];
            sliceInput.readBytes(values[position]);
        }

        return new VarcharArrayBlock(positionCount, values, offsets, lengths, valueIsNull);
    }
}
