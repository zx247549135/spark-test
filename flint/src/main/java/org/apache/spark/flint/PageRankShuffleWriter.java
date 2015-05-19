package org.apache.spark.flint;

import java.io.*;
import java.nio.channels.FileChannel;
import javax.annotation.Nullable;

import org.apache.spark.unsafe.memory.*;
import scala.Option;

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
import org.apache.spark.shuffle.IndexShuffleBlockManager;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.unsafe.PlatformDependent;

/**
 * Created by Administrator on 2015/5/18.
 */
public class PageRankShuffleWriter {

    private final Logger logger = LoggerFactory.getLogger(PageRankShuffleWriter.class);

    private final IndexShuffleBlockManager shuffleBlockManager;
    private final BlockManager blockManager;
    private final SparkConf sparkConf;
    //private final SerializerInstance serializer;

    //private final ShuffleWriteMetrics writeMetrics;
    private final int shuffleId;
    private final int mapId;
    private final int numPartitions;
    private final boolean transferToEnabled;

    private final ShuffleWriteMetrics writeMetrics;

    private boolean stopping = false;

    private PageRankMap map;
    private final MemoryBlock kvBuffer = MemoryBlock.fromLongArray(new long[2]);
    private final Object kvBaseObj = kvBuffer.getBaseObject();
    private final long kvBaseOffset = kvBuffer.getBaseOffset();

    public PageRankShuffleWriter(int shuffleId, int mapId, int numParts, TaskContext taskContext) throws IOException {
        this.shuffleId = shuffleId;
        this.mapId = mapId;
        this.numPartitions = numParts;

        SparkEnv env = SparkEnv.get();

        this.sparkConf = env.conf();
        this.blockManager = env.blockManager();
        this.shuffleBlockManager =  (IndexShuffleBlockManager) SparkEnv.get().shuffleManager().shuffleBlockManager();
        this.map = new PageRankMap(numPartitions, 1024, taskContext);

        this.writeMetrics = new ShuffleWriteMetrics();

        this.transferToEnabled = false;
    }

    public void write(long key, double value) throws IOException {
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

    public Option<MapStatus> stop(boolean success) throws IOException {
        try {
            if (stopping) {
                return Option.apply(null);
            } else {
                stopping = true;
                if (success) {
                    final SpillInfo[] spills = map.closeAndGetSpills();
                    map = null;
                    final long[] partitionLengths;
                    try {
                        partitionLengths = mergeSpills(spills);
                    } finally {
                        for (SpillInfo spill : spills) {
                            if (spill.file.exists() && ! spill.file.delete()) {
                                logger.error("Error while deleting spill file {}", spill.file.getPath());
                            }
                        }
                    }

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
            if (map != null) {
                map.cleanupAfterError();
            }
        }
    }

    /**
     * Merge zero or more spill files together, choosing the fastest merging strategy based on the
     * number of spills and the IO compression codec.
     *
     * @return the partition lengths in the merged file.
     */
    private long[] mergeSpills(SpillInfo[] spills) throws IOException {
        final File outputFile = shuffleBlockManager.getDataFile(shuffleId, mapId);
        final boolean compressionEnabled = sparkConf.getBoolean("spark.shuffle.compress", true);
        final CompressionCodec compressionCodec = CompressionCodec$.MODULE$.createCodec(sparkConf);
        final boolean fastMergeEnabled =
                sparkConf.getBoolean("spark.shuffle.unsafe.fastMergeEnabled", true);
        final boolean fastMergeIsSupported =
                !compressionEnabled || compressionCodec instanceof LZFCompressionCodec;
        try {
            if (spills.length == 0) {
                new FileOutputStream(outputFile).close(); // Create an empty file
                return new long[numPartitions];
            } else if (spills.length == 1) {
                // Here, we don't need to perform any metrics updates because the bytes written to this
                // output file would have already been counted as shuffle bytes written.
                Files.move(spills[0].file, outputFile);
                return spills[0].partitionLengths;
            } else {
                final long[] partitionLengths;
                // There are multiple spills to merge, so none of these spill files' lengths were counted
                // towards our shuffle write count or shuffle write time. If we use the slow merge path,
                // then the final output file's size won't necessarily be equal to the sum of the spill
                // files' sizes. To guard against this case, we look at the output file's actual size when
                // computing shuffle bytes written.
                //
                // We allow the individual merge methods to report their own IO times since different merge
                // strategies use different IO techniques.  We count IO during merge towards the shuffle
                // shuffle write time, which appears to be consistent with the "not bypassing merge-sort"
                // branch in ExternalSorter.
                if (fastMergeEnabled && fastMergeIsSupported) {
                    // Compression is disabled or we are using an IO compression codec that supports
                    // decompression of concatenated compressed streams, so we can perform a fast spill merge
                    // that doesn't need to interpret the spilled bytes.
                    if (transferToEnabled) {
                        logger.debug("Using transferTo-based fast merge");
                        partitionLengths = mergeSpillsWithTransferTo(spills, outputFile);
                    } else {
                        logger.debug("Using fileStream-based fast merge");
                        partitionLengths = mergeSpillsWithFileStream(spills, outputFile, null);
                    }
                } else {
                    logger.debug("Using slow merge");
                    partitionLengths = mergeSpillsWithFileStream(spills, outputFile, compressionCodec);
                }
                // When closing an UnsafeShuffleExternalSorter that has already spilled once but also has
                // in-memory records, we write out the in-memory records to a file but do not count that
                // final write as bytes spilled (instead, it's accounted as shuffle write). The merge needs
                // to be counted as shuffle write, but this will lead to double-counting of the final
                // SpillInfo's bytes.
                writeMetrics.decShuffleBytesWritten(spills[spills.length - 1].file.length());
                writeMetrics.incShuffleBytesWritten(outputFile.length());
                return partitionLengths;
            }
        } catch (IOException e) {
            if (outputFile.exists() && !outputFile.delete()) {
                logger.error("Unable to delete output file {}", outputFile.getPath());
            }
            throw e;
        }
    }

    /**
     * Merges spill files using Java FileStreams. This code path is slower than the NIO-based merge,
     * {@link PageRankShuffleWriter#mergeSpillsWithTransferTo(SpillInfo[], File)}, so it's only used in
     * cases where the IO compression codec does not support concatenation of compressed data, or in
     * cases where users have explicitly disabled use of {@code transferTo} in order to work around
     * kernel bugs.
     *
     * @param spills the spills to merge.
     * @param outputFile the file to write the merged data to.
     * @param compressionCodec the IO compression codec, or null if shuffle compression is disabled.
     * @return the partition lengths in the merged file.
     */
    private long[] mergeSpillsWithFileStream(
            SpillInfo[] spills,
            File outputFile,
            @Nullable CompressionCodec compressionCodec) throws IOException {
        assert (spills.length >= 2);
        final long[] partitionLengths = new long[numPartitions];
        final InputStream[] spillInputStreams = new FileInputStream[spills.length];
        OutputStream mergedFileOutputStream = null;

        boolean threwException = true;
        try {
            for (int i = 0; i < spills.length; i++) {
                spillInputStreams[i] = new FileInputStream(spills[i].file);
            }
            for (int partition = 0; partition < numPartitions; partition++) {
                final long initialFileLength = outputFile.length();
                mergedFileOutputStream = new FileOutputStream(outputFile, true);
                        // new TimeTrackingOutputStream(writeMetrics, new FileOutputStream(outputFile, true));
                if (compressionCodec != null) {
                    mergedFileOutputStream = compressionCodec.compressedOutputStream(mergedFileOutputStream);
                }

                for (int i = 0; i < spills.length; i++) {
                    final long partitionLengthInSpill = spills[i].partitionLengths[partition];
                    if (partitionLengthInSpill > 0) {
                        InputStream partitionInputStream =
                                new LimitedInputStream(spillInputStreams[i], partitionLengthInSpill);
                        if (compressionCodec != null) {
                            partitionInputStream = compressionCodec.compressedInputStream(partitionInputStream);
                        }
                        ByteStreams.copy(partitionInputStream, mergedFileOutputStream);
                    }
                }
                mergedFileOutputStream.flush();
                mergedFileOutputStream.close();
                partitionLengths[partition] = (outputFile.length() - initialFileLength);
            }
            threwException = false;
        } finally {
            // To avoid masking exceptions that caused us to prematurely enter the finally block, only
            // throw exceptions during cleanup if threwException == false.
            for (InputStream stream : spillInputStreams) {
                Closeables.close(stream, threwException);
            }
            Closeables.close(mergedFileOutputStream, threwException);
        }
        return partitionLengths;
    }

    /**
     * Merges spill files by using NIO's transferTo to concatenate spill partitions' bytes.
     * This is only safe when the IO compression codec and serializer support concatenation of
     * serialized streams.
     *
     * @return the partition lengths in the merged file.
     */
    private long[] mergeSpillsWithTransferTo(SpillInfo[] spills, File outputFile) throws IOException {
        assert (spills.length >= 2);
        final long[] partitionLengths = new long[numPartitions];
        final FileChannel[] spillInputChannels = new FileChannel[spills.length];
        final long[] spillInputChannelPositions = new long[spills.length];
        FileChannel mergedFileOutputChannel = null;

        boolean threwException = true;
        try {
            for (int i = 0; i < spills.length; i++) {
                spillInputChannels[i] = new FileInputStream(spills[i].file).getChannel();
            }
            // This file needs to opened in append mode in order to work around a Linux kernel bug that
            // affects transferTo; see SPARK-3948 for more details.
            mergedFileOutputChannel = new FileOutputStream(outputFile, true).getChannel();

            long bytesWrittenToMergedFile = 0;
            for (int partition = 0; partition < numPartitions; partition++) {
                for (int i = 0; i < spills.length; i++) {
                    final long partitionLengthInSpill = spills[i].partitionLengths[partition];
                    long bytesToTransfer = partitionLengthInSpill;
                    final FileChannel spillInputChannel = spillInputChannels[i];
                    final long writeStartTime = System.nanoTime();
                    while (bytesToTransfer > 0) {
                        final long actualBytesTransferred = spillInputChannel.transferTo(
                                spillInputChannelPositions[i],
                                bytesToTransfer,
                                mergedFileOutputChannel);
                        spillInputChannelPositions[i] += actualBytesTransferred;
                        bytesToTransfer -= actualBytesTransferred;
                    }
                    writeMetrics.incShuffleWriteTime(System.nanoTime() - writeStartTime);
                    bytesWrittenToMergedFile += partitionLengthInSpill;
                    partitionLengths[partition] += partitionLengthInSpill;
                }
            }
            // Check the position after transferTo loop to see if it is in the right position and raise an
            // exception if it is incorrect. The position will not be increased to the expected length
            // after calling transferTo in kernel version 2.6.32. This issue is described at
            // https://bugs.openjdk.java.net/browse/JDK-7052359 and SPARK-3948.
            if (mergedFileOutputChannel.position() != bytesWrittenToMergedFile) {
                throw new IOException(
                        "Current position " + mergedFileOutputChannel.position() + " does not equal expected " +
                                "position " + bytesWrittenToMergedFile + " after transferTo. Please check your kernel" +
                                " version to see if it is 2.6.32, as there is a kernel bug which will lead to " +
                                "unexpected behavior when using transferTo. You can set spark.file.transferTo=false " +
                                "to disable this NIO feature."
                );
            }
            threwException = false;
        } finally {
            // To avoid masking exceptions that caused us to prematurely enter the finally block, only
            // throw exceptions during cleanup if threwException == false.
            for (int i = 0; i < spills.length; i++) {
                assert(spillInputChannelPositions[i] == spills[i].file.length());
                Closeables.close(spillInputChannels[i], threwException);
            }
            Closeables.close(mergedFileOutputChannel, threwException);
        }
        return partitionLengths;
    }

}
