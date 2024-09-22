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

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.NodeProvider;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.planner.plan.physical.output.ScanOutput;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * @author tao
 * @author hank
 * @date 2018-01-20 19:15
 */
public class PixelsSplit implements ConnectorSplit
{
    private final long transId;
    private final long splitId;
    private final String connectorId;
    private final String schemaName;
    private final String tableName;
    private String storageScheme;
    private List<String> paths;
    private List<Integer> rgStarts;
    private List<Integer> rgLengths;
    private int pathIndex;
    private boolean cached;
    private final boolean ensureLocality;
    private final List<HostAddress> addresses;
    private List<String> columnOrder;
    private List<String> cacheOrder;
    private final TupleDomain<PixelsColumnHandle> constraint;

    @JsonCreator
    public PixelsSplit(
            @JsonProperty("transId") long transId,
            @JsonProperty("splitId") long splitId,
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("storageScheme") String storageScheme,
            @JsonProperty("paths") List<String> paths,
            @JsonProperty("rgStarts") List<Integer> rgStarts,
            @JsonProperty("rgLengths") List<Integer> rgLengths,
            @JsonProperty("cached") boolean cached,
            @JsonProperty("ensureLocality") boolean ensureLocality,
            @JsonProperty("addresses") List<HostAddress> addresses,
            @JsonProperty("columnOrder") List<String> columnOrder,
            @JsonProperty("cacheOrder") List<String> cacheOrder,
            @JsonProperty("constraint") TupleDomain<PixelsColumnHandle> constraint) {
        this.transId = transId;
        this.splitId = splitId;
        this.schemaName = requireNonNull(schemaName, "schema name is null");
        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.tableName = requireNonNull(tableName, "table name is null");
        this.storageScheme = requireNonNull(storageScheme, "storage scheme is null");
        this.paths = requireNonNull(paths, "paths is null");
        checkArgument(!paths.isEmpty(), "paths is empty");
        this.pathIndex = 0;
        this.rgStarts = requireNonNull(rgStarts, "rgStarts is null");
        checkArgument(rgStarts.size() == paths.size(),
                "the size of rgStarts and paths are different");
        this.rgLengths = requireNonNull(rgLengths, "rgLengths is null");
        checkArgument(rgLengths.size() == paths.size(),
                "the size of rgLengths and paths are different");
        this.cached = cached;
        this.ensureLocality = ensureLocality;
        this.addresses = ImmutableList.copyOf(requireNonNull(addresses, "addresses is null"));
        this.columnOrder = requireNonNull(columnOrder, "order is null");
        this.cacheOrder = requireNonNull(cacheOrder, "cache order is null");
        this.constraint = requireNonNull(constraint, "constraint is null");
    }

    /**
     * Permute the original file information with the information of the
     * intermediate files produced by serverless.
     * @param scheme the storage scheme of intermediate files
     * @param scanOutput the output of serverless
     */
    public void permute(Storage.Scheme scheme, ScanOutput scanOutput)
    {
        requireNonNull(scheme, "scheme is null");
        requireNonNull(scanOutput, "scanOutput is null");
        requireNonNull(scanOutput.getOutputs(), "scanOutput.outputs is null");
        requireNonNull(scanOutput.getRowGroupNums(), "scanOutput.rowGroupNums is null");
        this.storageScheme = scheme.name();
        this.paths = scanOutput.getOutputs();
        this.rgStarts = Collections.nCopies(scanOutput.getOutputs().size(), 0);
        this.rgLengths = scanOutput.getRowGroupNums();
        this.cached = false;
        if (!this.columnOrder.isEmpty())
            this.columnOrder = ImmutableList.of();
        if (!this.cacheOrder.isEmpty())
            this.cacheOrder = ImmutableList.of();
    }

    @JsonProperty
    public String getConnectorId() {
        return connectorId;
    }

    @JsonProperty
    public String getSchemaName() {
        return schemaName;
    }

    @JsonProperty
    public TupleDomain<PixelsColumnHandle> getConstraint() {
        return constraint;
    }

    @JsonProperty
    public String getTableName() {
        return tableName;
    }

    @JsonProperty
    public String getStorageScheme() {
        return storageScheme;
    }

    @JsonProperty
    public List<String> getPaths() {
        return paths;
    }

    @JsonProperty
    public long getTransId() {
        return transId;
    }

    @JsonProperty
    public long getSplitId()
    {
        return splitId;
    }

    @JsonProperty
    public List<Integer> getRgStarts()
    {
        return rgStarts;
    }

    @JsonProperty
    public List<Integer> getRgLengths()
    {
        return rgLengths;
    }

    @JsonProperty
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @JsonProperty
    public boolean getEnsureLocality()
    {
        return ensureLocality;
    }

    @JsonProperty
    public boolean getCached()
    {
        return cached;
    }

    public boolean nextPath()
    {
        if (this.pathIndex+1 < this.paths.size())
        {
            this.pathIndex++;
            return true;
        }
        else
        {
            return false;
        }
    }

    public boolean isEmpty()
    {
        return this.paths.isEmpty();
    }

    public String getPath()
    {
        return this.paths.get(this.pathIndex);
    }

    public int getRgStart() {
        return this.rgStarts.get(pathIndex);
    }

    public int getRgLength() {
        return this.rgLengths.get(pathIndex);
    }

    @JsonProperty
    public List<String> getColumnOrder()
    {
        return columnOrder;
    }

    @JsonProperty
    public List<String> getCacheOrder()
    {
        return cacheOrder;
    }

    @Override
    public NodeSelectionStrategy getNodeSelectionStrategy()
    {
        /**
         * PIXELS-222:
         * Some storage systems, such as S3, does not provide data
         * locality. We should not force Presto to access local data.
         */
        if (ensureLocality)
        {
            return NodeSelectionStrategy.HARD_AFFINITY;
        }
        return NodeSelectionStrategy.NO_PREFERENCE;
    }

    @Override
    public List<HostAddress> getPreferredNodes(NodeProvider nodeProvider)
    {
        return addresses;
    }

    @Override
    public Object getInfo() {
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PixelsSplit that = (PixelsSplit) o;

        return this.transId == that.transId && this.splitId == that.splitId &&
                Objects.equals(this.connectorId, that.connectorId) &&
                Objects.equals(this.schemaName, that.schemaName) &&
                Objects.equals(this.tableName, that.tableName) &&
                Objects.equals(this.paths, that.paths) &&
                Objects.equals(this.rgStarts, that.rgStarts) &&
                Objects.equals(this.rgLengths, that.rgLengths) &&
                Objects.equals(this.addresses, that.addresses) &&
                // No need to consider this.order and this.cacheOrder.
                Objects.equals(this.constraint, that.constraint);
    }

    @Override
    public int hashCode() {
        // No need to consider this.order and this.cacheOrder.
        return Objects.hash(transId, splitId, connectorId, schemaName, tableName,
                paths, rgStarts, rgLengths, addresses, cached, constraint);
    }

    @Override
    public String toString() {
        // No need to print order, cacheOrder, and constraint, in most cases.
        return "PixelsSplit{" +
                "transId=" + transId + ", splitId=" + splitId +
                ", connectorId='" + connectorId + '\'' +
                ", schemaName='" + schemaName + '\'' +
                ", tableName='" + tableName + '\'' +
                ", storageScheme='" + storageScheme + '\'' +
                ", paths=" + listToJsonArray(paths) +
                ", rgStarts=" + listToJsonArray(rgStarts) +
                ", rgLengths=" + listToJsonArray(rgLengths) +
                ", isCached=" + cached +
                ", addresses=" + addresses +
                '}';
    }

    private <T> String listToJsonArray(List<T> list)
    {
        StringBuilder builder = new StringBuilder("[");
        if (!list.isEmpty())
        {
            builder.append(list.get(0));
            for (int i = 1; i < list.size(); ++i)
            {
                builder.append(",").append(list.get(i));
            }
        }
        builder.append("]");
        return builder.toString();
    }
}
