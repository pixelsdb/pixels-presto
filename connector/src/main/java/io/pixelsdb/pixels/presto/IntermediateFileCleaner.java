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

import io.airlift.log.Logger;
import io.pixelsdb.pixels.common.physical.Storage;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

/**
 * @author hank
 * @date 22/04/2022
 */
public class IntermediateFileCleaner implements Runnable
{
    private static final Logger logger = Logger.get(PixelsPageSource.class);
    private static final ExecutorService cleanerService = Executors.newSingleThreadExecutor();
    private static final IntermediateFileCleaner cleaner = new IntermediateFileCleaner();

    static
    {
        cleanerService.execute(cleaner);
        cleanerService.shutdown();
        Runtime.getRuntime().addShutdownHook(new Thread(cleanerService::shutdownNow));
    }

    public static IntermediateFileCleaner Instance()
    {
        return cleaner;
    }

    private final LinkedBlockingQueue<String> pathsToDelete = new LinkedBlockingQueue<>();
    private Storage storage = null;

    private IntermediateFileCleaner() { }

    public void asyncDelete(String path, Storage storage) throws InterruptedException
    {
        requireNonNull(path, "paths is null");
        requireNonNull(storage, "storage is null");
        if (this.storage == null)
        {
            this.storage = storage;
        }
        this.pathsToDelete.put(path);
    }

    @Override
    public void run()
    {
        while (true)
        {
            String path = null;
            try
            {
                path = this.pathsToDelete.poll(1, TimeUnit.SECONDS);
                if (path != null)
                {
                    requireNonNull(storage, "storage is null");
                    if (storage.delete(path, false))
                    {
                        logger.info("intermediate file '" + path + "' is deleted.");
                    }
                    else
                    {
                        logger.error("failed to delete intermediate file: " + path);
                    }
                }
            } catch (Exception e)
            {
                logger.error(e, "failed to delete intermediate file: " + path);
            }
        }
    }
}
