package org.apache.spark.flint;

import java.io.File;

import org.apache.spark.storage.TempShuffleBlockId;

final class SpillInfo {
    final long[] partitionLengths;
    final File file;
    final TempShuffleBlockId blockId;

    public SpillInfo(int numPartitions, File file, TempShuffleBlockId blockId) {
        this.partitionLengths = new long[numPartitions];
        this.file = file;
        this.blockId = blockId;
    }
}
