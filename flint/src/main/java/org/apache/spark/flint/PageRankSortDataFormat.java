package org.apache.spark.flint;

import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.util.collection.SortDataFormat;

import java.util.Objects;

final class PageRankSortDataFormat extends SortDataFormat<PageRankPointer, LongArray> {

    public static final PageRankSortDataFormat INSTANCE = new PageRankSortDataFormat();

    private PageRankSortDataFormat() { }

    @Override
    public PageRankPointer getKey(LongArray data, int pos) {
        // Since we re-use keys, this method shouldn't be called.
        throw new UnsupportedOperationException();
    }

    @Override
    public PageRankPointer newKey() {
        return new PageRankPointer();
    }

    @Override
    public PageRankPointer getKey(LongArray data, int pos, PageRankPointer reuse) {
        reuse.key = data.get(pos * 2);
        reuse.packedCodes = data.get(pos * 2 + 1);
        reuse.partitionId = data.getHighHalf(pos *2 + 1);
        reuse.hashcode = data.getLowHalf(pos *2 + 1);
        return reuse;
    }

    @Override
    public void swap(LongArray data, int pos0, int pos1) {
        long temp = data.get(pos0 * 2);
        data.set(pos0 * 2, data.get(pos1 * 2));
        data.set(pos1 * 2, temp);

        temp = data.get(pos0 * 2 + 1);
        data.set(pos0 * 2 + 1, data.get(pos1 * 2 + 1));
        data.set(pos1 * 2 + 1, temp);
    }

    @Override
    public void copyElement(LongArray src, int srcPos, LongArray dst, int dstPos) {
        dst.set(dstPos * 2, src.get(srcPos * 2));
        dst.set(dstPos * 2 + 1, src.get(srcPos * 2 + 1));
    }

    @Override
    public void copyRange(LongArray src, int srcPos, LongArray dst, int dstPos, int length) {
        Object srcBase = src.memoryBlock().getBaseObject();
        Object dstBase = dst.memoryBlock().getBaseObject();
        System.arraycopy(srcBase, srcPos * 2, dstBase, dstPos * 2, length * 2);
    }

    @Override
    public LongArray allocate(int length) {
        return new LongArray(MemoryBlock.fromLongArray(new long[length * 2]));
    }

}
