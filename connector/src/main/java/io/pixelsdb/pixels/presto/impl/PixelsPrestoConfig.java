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
package io.pixelsdb.pixels.presto.impl;

import com.facebook.presto.spi.PrestoException;
import io.airlift.configuration.Config;
import io.airlift.log.Logger;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.presto.exception.PixelsErrorCode;

import javax.validation.constraints.NotNull;
import java.io.IOException;

/**
 * @Description: Configuration read from etc/catalog/pixels.properties
 * @author: tao
 * @date: Create in 2018-01-20 11:16
 **/
public class PixelsPrestoConfig
{
    private Logger logger = Logger.get(PixelsPrestoConfig.class);
    private ConfigFactory configFactory = null;

    private static int BatchSize = 1000;

    public static int getBatchSize()
    {
        return BatchSize;
    }

    private String pixelsConfig = null;

    @Config("pixels.config")
    public PixelsPrestoConfig setPixelsConfig (String pixelsConfig)
    {
        this.pixelsConfig = pixelsConfig;

        // reload configuration
        if (this.configFactory == null)
        {
            if (pixelsConfig == null || pixelsConfig.isEmpty())
            {
                String pixelsHome = ConfigFactory.Instance().getProperty("pixels.home");
                if (pixelsHome == null)
                {
                    logger.info("using pixels.properties in jar.");
                } else
                {
                    logger.info("using pixels.properties under default pixels.home: " + pixelsHome);
                }
            } else
            {
                try
                {
                    ConfigFactory.Instance().loadProperties(pixelsConfig);
                    logger.info("using pixels.properties specified by the connector: " + pixelsConfig);

                } catch (IOException e)
                {
                    logger.error(e,"can not load pixels.properties: " + pixelsConfig +
                            ", configuration reloading is skipped.");
                    throw new PrestoException(PixelsErrorCode.PIXELS_CONFIG_ERROR, e);
                }
            }

            this.configFactory = ConfigFactory.Instance();
            try
            {
                int batchSize = Integer.parseInt(this.configFactory.getProperty("row.batch.size"));
                if (batchSize > 0)
                {
                    BatchSize = batchSize;
                    logger.info("using pixels row.batch.size: " + BatchSize);
                }
            } catch (NumberFormatException e)
            {
                throw new PrestoException(PixelsErrorCode.PIXELS_CONFIG_ERROR, e);
            }
            try
            {
                /**
                 * PIXELS-108:
                 * We reload the storage here, because in other classes like
                 * PixelsSplitManager, when we try to create storage instance
                 * by StorageFactory.Instance().getStorage(), Presto does not
                 * load the dependencies (e.g. hadoop-hdfs-xxx.jar) of the storage
                 * implementation (e.g. io.pixelsdb.pixels.common.physical.storage.HDFS).
                 *
                 * I currently don't know the reason (08.27.2021).
                 */
                StorageFactory.Instance().reload();
            } catch (IOException e)
            {
                throw new PrestoException(PixelsErrorCode.PIXELS_STORAGE_ERROR, e);
            }
        }
        return this;
    }

    @NotNull
    public String getPixelsConfig ()
    {
        return this.pixelsConfig;
    }

    /**
     * Injected class should get ConfigFactory instance by this method instead of ConfigFactory.Instance().
     * @return
     */
    @NotNull
    public ConfigFactory getConfigFactory()
    {
        return this.configFactory;
    }
}
