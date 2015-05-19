package org.apache.spark.flint;

import java.io.*;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import javax.annotation.Nullable;

import org.apache.spark.storage.ShuffleBlockId;
import org.apache.spark.unsafe.memory.*;
import scala.Option;
import scala.Product2;
import scala.collection.JavaConversions;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.*;
import org.apache.spark.io.CompressionCodec;
import org.apache.spark.io.CompressionCodec$;
import org.apache.spark.io.LZFCompressionCodec;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.network.util.LimitedInputStream;
import org.apache.spark.scheduler.MapStatus;
import org.apache.spark.scheduler.MapStatus$;
import org.apache.spark.serializer.SerializationStream;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.shuffle.IndexShuffleBlockManager;
import org.apache.spark.shuffle.ShuffleMemoryManager;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.unsafe.PlatformDependent;

/**
 * Created by Administrator on 2015/5/18.
 */
public class PageRankShuffleWriter {

    private final IndexShuffleBlockManager shuffleBlockManager;
    private final BlockManager blockManager;
    //private final SerializerInstance serializer;

    //private final ShuffleWriteMetrics writeMetrics;
    private final int shuffleId;
    private final int mapId;
    private final int numPart;
    private final TaskContext taskContext;
    private final SparkConf sparkConf;
    //private final boolean transferToEnabled;

    private boolean stopping = false;

    private final PageRankMap map;
    private final MemoryBlock kvBuffer = MemoryBlock.fromLongArray(new long[2]);
    private final Object kvBaseObj = kvBuffer.getBaseObject();
    private final long kvBaseOffset = kvBuffer.getBaseOffset();

    public PageRankShuffleWriter(int shuffleId, int mapId, int numParts, TaskContext taskContext) throws IOException {
        this.shuffleId = shuffleId;
        this.mapId = mapId;
        this.numPart = numParts;

        SparkEnv env = SparkEnv.get();
        this.taskContext = taskContext;
        this.sparkConf = env.conf();

        this.blockManager = env.blockManager();
        this.shuffleBlockManager =  (IndexShuffleBlockManager) SparkEnv.get().shuffleManager().shuffleBlockManager();
        this.map = new PageRankMap(numPart, 1024, taskContext);
    }

    public void write(long key, double value) {
        PlatformDependent.UNSAFE.putLong(kvBaseObj, kvBaseOffset, key);
        PlatformDependent.UNSAFE.putDouble(kvBaseObj, kvBaseOffset + 8, value);
        PageRankMap.Location loc = map.lookup(kvBaseObj, kvBaseOffset);
        if (loc.isDefined()) {
            MemoryLocation valueLoc = loc.getValueAddress();
            value += PlatformDependent.UNSAFE.getDouble(valueLoc.getBaseObject(), valueLoc.getBaseOffset());
            PlatformDependent.UNSAFE.putDouble(valueLoc.getBaseObject(), valueLoc.getBaseOffset(), value);
        } else {
            loc.putNewKey(kvBaseObj, kvBaseOffset);
        }
    }

    public Option<MapStatus> stop(boolean success) {
        try {
            if (stopping) {
                return Option.apply(null);
            } else {
                stopping = true;
                if (success) {
                    File outputFile = shuffleBlockManager.getDataFile(shuffleId, mapId);
                    ShuffleBlockId blockId = shuffleBlockManager.consolidateId(shuffleId, mapId);
                    long[] partitionLengths = new long[1]; //sorter.writePartitionedFile(blockId, context, outputFile)
                    shuffleBlockManager.writeIndexFile(shuffleId, mapId, partitionLengths);

                    MapStatus mapStatus = MapStatus$.MODULE$.apply(blockManager.shuffleServerId(), partitionLengths);
                    return Option.apply(mapStatus);
                } else {
                    // The map task failed, so delete our output data.
                    shuffleBlockManager.removeDataByMap(shuffleId, mapId);
                    return Option.apply(null);
                }
            }
        } finally {

        }
    }

}
