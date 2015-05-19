package org.apache.spark.flint;

import java.io.File;
import java.io.IOException;
import java.lang.Override;
import java.lang.UnsupportedOperationException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.shuffle.IndexShuffleBlockManager;
import org.apache.spark.shuffle.ShuffleMemoryManager;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.storage.BlockObjectWriter;
import org.apache.spark.storage.TempShuffleBlockId;
import org.apache.spark.util.Utils;
import org.apache.spark.unsafe.*;
import org.apache.spark.unsafe.array.ByteArrayMethods;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.unsafe.bitset.BitSet;
import org.apache.spark.unsafe.hash.Murmur3_x86_32;
import org.apache.spark.unsafe.map.HashMapGrowthStrategy;
import org.apache.spark.unsafe.memory.*;
import org.apache.spark.util.collection.Sorter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

/**
 * An append-only hash map where keys and values are contiguous regions of bytes.
 * <p>
 * This is backed by a power-of-2-sized hash table, using quadratic probing with triangular numbers,
 * which is guaranteed to exhaust the space.
 * <p>
 * The map can support up to 2^31 keys because we use 32 bit MurmurHash. If the key cardinality is
 * higher than this, you should probably be using sorting instead of hashing for better cache
 * locality.
 * <p>
 * This class is not thread safe.
 */
public final class PageRankMap {

    private final Logger logger = LoggerFactory.getLogger(PageRankMap.class);

    private static final Murmur3_x86_32 HASHER = new Murmur3_x86_32(0);

    private static final HashMapGrowthStrategy growthStrategy = HashMapGrowthStrategy.DOUBLING;

    private final BlockManager blockManager;
    private final IndexShuffleBlockManager shuffleBlockManager;
    private final TaskMemoryManager memoryManager;
    private final ShuffleMemoryManager shuffleMemoryManager;

    private final ShuffleWriteMetrics writeMetrics;

    /**
     * A linked list for tracking all allocated data pages so that we can free all of our memory.
     */
    private final List<MemoryBlock> dataPages = new LinkedList<MemoryBlock>();

    /**
     * The data page that will be used to store keys and values for new hashtable entries. When this
     * page becomes full, a new page will be allocated and this pointer will change to point to that
     * new page.
     */
    private MemoryBlock currentDataPage = null;

    /**
     * Offset into `currentDataPage` that points to the location where new data can be inserted into
     * the page.
     */
    private long pageCursor = 0;

    /**
     * The size of the data pages that hold key and value data. Map entries cannot span multiple
     * pages, so this limits the maximum entry size.
     */
    private static final long PAGE_SIZE_BYTES = 1L << 26; // 64 megabytes

    private static final int KV_LENGTH_BYTES = 8;

    // This choice of page table size and page size means that we can address up to 500 gigabytes
    // of memory.

    /**
     * A single array to store the key and value.
     *
     * Position {@code 2 * i} in the array is used to track a pointer to the key at index {@code i},
     * while position {@code 2 * i + 1} in the array holds key's full 32-bit hashcode.
     */
    private LongArray longArray;
    // TODO: we're wasting 32 bits of space here; we can probably store fewer bits of the hashcode
    // and exploit word-alignment to use fewer bits to hold the address.  This might let us store
    // only one long per map entry, increasing the chance that this array will fit in cache at the
    // expense of maybe performing more lookups if we have hash collisions.  Say that we stored only
    // 27 bits of the hashcode and 37 bits of the address.  37 bits is enough to address 1 terabyte
    // of RAM given word-alignment.  If we use 13 bits of this for our page table, that gives us a
    // maximum page size of 2^24 * 8 = ~134 megabytes per page. This change will require us to store
    // full base addresses in the page table for off-heap mode so that we can reconstruct the full
    // absolute memory addresses.

    /**
     * A {@link BitSet} used to track location of the map where the key is set.
     * Size of the bitset should be half of the size of the long array.
     */
    private BitSet bitset;

    private final double loadFactor;

    private final int numPartitions;

    private final int initialSize;


    /**
     * Number of keys defined in the map.
     */
    private int size;

    /**
     * The map will be expanded once the number of keys exceeds this threshold.
     */
    private int growthThreshold;

    /**
     * Mask for truncating hashcodes so that they do not exceed the long array's size.
     * This is a strength reduction optimization; we're essentially performing a modulus operation,
     * but doing so with a bitmask because this is a power-of-2-sized hash map.
     */
    private int mask;

    /**
     * Return value of {@link PageRankMap#lookup(Object, long)}.
     */
    private final Location loc;

    private final boolean enablePerfMetrics;

    private long timeSpentResizingNs = 0;

    private long numProbes = 0;

    private long numKeyLookups = 0;

    private long numHashCollisions = 0;

    public PageRankMap(
            int numParts,
            int initialCapacity,
            double loadFactor,
            boolean enablePerfMetrics,
            TaskContext taskContext) throws IOException {
        this.taskContext = taskContext;

        this.numPartitions = numParts;
        this.loadFactor = loadFactor;
        this.loc = new Location();
        this.enablePerfMetrics = enablePerfMetrics;
        this.initialSize = initialCapacity;

        SparkEnv env = SparkEnv.get();
        this.blockManager = env.blockManager();
        this.shuffleBlockManager =  (IndexShuffleBlockManager) SparkEnv.get().shuffleManager().shuffleBlockManager();
        this.memoryManager = new TaskMemoryManager(new ExecutorMemoryManager(MemoryAllocator.HEAP));
        this.shuffleMemoryManager = env.shuffleMemoryManager();

        this.writeMetrics = new ShuffleWriteMetrics();

        // for sort and spill

        this.fileBufferSizeBytes = env.conf().getInt("spark.shuffle.file.buffer", 32) * 1024;

        initializeForWriting();
    }

    public PageRankMap(int numParts, int initialCapacity, TaskContext taskContext) throws IOException {
        this(numParts, initialCapacity, 0.70, false, taskContext);
    }

    public PageRankMap(
            int numParts,
            int initialCapacity,
            boolean enablePerfMetrics,
            TaskContext taskContext) throws IOException {
        this(numParts, initialCapacity, 0.70, enablePerfMetrics, taskContext);
    }

    /**
     * Returns the number of keys defined in the map.
     */
    public int size() { return size; }

    /**
     * Returns an iterator for iterating over the entries of this map.
     *
     * For efficiency, all calls to `next()` will return the same {@link Location} object.
     *
     * If any other lookups or operations are performed on this map while iterating over it, including
     * `lookup()`, the behavior of the returned iterator is undefined.
     */
    public Iterator<Location> iterator() {
        return new Iterator<Location>() {

            private int nextPos = bitset.nextSetBit(0);

            @Override
            public boolean hasNext() {
                return nextPos != -1;
            }

            @Override
            public Location next() {
                final int pos = nextPos;
                nextPos = bitset.nextSetBit(nextPos + 1);
                return loc.with(pos, 0, true);
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    /**
     * Looks up a key, and return a {@link Location} handle that can be used to test existence
     * and read/write values.
     *
     * This function always return the same {@link Location} instance to avoid object allocation.
     */
    public Location lookup(
            Object keyBaseObject,
            long keyBaseOffset) {
        if (enablePerfMetrics) {
            numKeyLookups++;
        }
        final int hashcode = HASHER.hashUnsafeWords(keyBaseObject, keyBaseOffset, KV_LENGTH_BYTES);
        int pos = hashcode & mask;
        int step = 1;
        while (true) {
            if (enablePerfMetrics) {
                numProbes++;
            }
            if (!bitset.isSet(pos)) {
                // This is a new key.
                return loc.with(pos, hashcode, false);
            } else {
                int stored = longArray.getLowHalf(pos * 2 + 1);
                if (stored == hashcode) {
                    // Full hash code matches.  Let's compare the keys for equality.
                    loc.with(pos, hashcode, true);

                    final MemoryLocation keyAddress = loc.getKeyAddress();
                    final Object storedKeyBaseObject = keyAddress.getBaseObject();
                    final long storedKeyBaseOffset = keyAddress.getBaseOffset();
                    final boolean areEqual = ByteArrayMethods.wordAlignedArrayEquals(
                            keyBaseObject,
                            keyBaseOffset,
                            storedKeyBaseObject,
                            storedKeyBaseOffset,
                            KV_LENGTH_BYTES
                    );
                    if (areEqual) {
                        return loc;
                    } else {
                        if (enablePerfMetrics) {
                            numHashCollisions++;
                        }
                    }
                }
            }
            pos = (pos + step) & mask;
            step++;
        }
    }

    /**
     * Handle returned by {@link PageRankMap#lookup(Object, long)} function.
     */
    public final class Location {
        /** An index into the hash map's Long array */
        private int pos;
        /** True if this location points to a position where a key is defined, false otherwise */
        private boolean isDefined;
        /**
         * The hashcode of the most recent key passed to
         * {@link PageRankMap#lookup(Object, long)}. Caching this hashcode here allows us to
         * avoid re-hashing the key when storing a value for that key.
         */
        private int keyHashcode;
        private final MemoryLocation keyMemoryLocation = new MemoryLocation();
        private final MemoryLocation valueMemoryLocation = new MemoryLocation();

        private void updateAddressesAndSizes(long fullKeyAddress) {
            final Object page = memoryManager.getPage(fullKeyAddress);
            final long keyOffsetInPage = memoryManager.getOffsetInPage(fullKeyAddress);
            long position = keyOffsetInPage;
            keyMemoryLocation.setObjAndOffset(page, position);
            position += KV_LENGTH_BYTES;
            valueMemoryLocation.setObjAndOffset(page, position);
        }

        Location with(int pos, int keyHashcode, boolean isDefined) {
            this.pos = pos;
            this.isDefined = isDefined;
            this.keyHashcode = keyHashcode;
            if (isDefined) {
                final long fullKeyAddress = longArray.get(pos * 2);
                updateAddressesAndSizes(fullKeyAddress);
            }
            return this;
        }

        /**
         * Returns true if the key is defined at this position, and false otherwise.
         */
        public boolean isDefined() {
            return isDefined;
        }

        /**
         * Returns the address of the key defined at this position.
         * This points to the first byte of the key data.
         * Unspecified behavior if the key is not defined.
         * For efficiency reasons, calls to this method always returns the same MemoryLocation object.
         */
        public MemoryLocation getKeyAddress() {
            assert (isDefined);
            return keyMemoryLocation;
        }

        /**
         * Returns the address of the value defined at this position.
         * This points to the first byte of the value data.
         * Unspecified behavior if the key is not defined.
         * For efficiency reasons, calls to this method always returns the same MemoryLocation object.
         */
        public MemoryLocation getValueAddress() {
            assert (isDefined);
            return valueMemoryLocation;
        }

        /**
         * Store a new key and value. This method may only be called once for a given key; if you want
         * to update the value associated with a key, then you can directly manipulate the bytes stored
         * at the value address.
         * <p>
         * It is only valid to call this method immediately after calling `lookup()` using the same key.
         * <p>
         * After calling this method, calls to `get[Key|Value]Address()` and `get[Key|Value]Length`
         * will return information on the data stored by this `putNewKey` call.
         * <p>
         * As an example usage, here's the proper way to store a new key:
         * <p>
         * <pre>
         *   Location loc = map.lookup(keyBaseObject, keyBaseOffset, keyLengthInBytes);
         *   if (!loc.isDefined()) {
         *     loc.putNewKey(keyBaseObject, keyBaseOffset, keyLengthInBytes, ...)
         *   }
         * </pre>
         * <p>
         * Unspecified behavior if the key is not defined.
         */
        public void putNewKey(
                Object keyBaseObject,
                long keyBaseOffset,
                Object valueBaseObject,
                long valueBaseOffset) {
            assert (!isDefined) : "Can only set value once for a key";
            isDefined = true;
            // Here, we'll copy the data into our data pages. Because we only store a relative offset from
            // the key address instead of storing the absolute address of the value, the key and value
            // must be stored in the same memory page.
            // (8 byte key length) (key) (8 byte value length) (value)
            final long requiredSize = KV_LENGTH_BYTES + KV_LENGTH_BYTES;
            size++;
            bitset.set(pos);

            // If there's not enough space in the current page, allocate a new page:
            if (currentDataPage == null || PAGE_SIZE_BYTES - pageCursor < requiredSize) {
                MemoryBlock newPage = memoryManager.allocatePage(PAGE_SIZE_BYTES);
                dataPages.add(newPage);
                pageCursor = 0;
                currentDataPage = newPage;
            }

            // Compute all of our offsets up-front:
            final Object pageBaseObject = currentDataPage.getBaseObject();
            final long pageBaseOffset = currentDataPage.getBaseOffset();
            final long keyDataOffsetInPage = pageBaseOffset + pageCursor;
            pageCursor += KV_LENGTH_BYTES;
            final long valueDataOffsetInPage = pageBaseOffset + pageCursor;
            pageCursor += KV_LENGTH_BYTES;

            // Copy the key
            PlatformDependent.copyMemory(
                    keyBaseObject, keyBaseOffset, pageBaseObject, keyDataOffsetInPage, KV_LENGTH_BYTES);
            // Copy the value
            PlatformDependent.copyMemory(
                    valueBaseObject, valueBaseOffset, pageBaseObject, valueDataOffsetInPage, KV_LENGTH_BYTES);

            final long storedKeyAddress = memoryManager.encodePageNumberAndOffset(
                    currentDataPage, keyDataOffsetInPage);
            longArray.set(pos * 2, storedKeyAddress);
            longArray.setLowHalf(pos * 2 + 1, keyHashcode);
            longArray.setHighHalf(pos * 2 + 1, Utils.nonNegativeMod(keyHashcode, numPartitions));
            updateAddressesAndSizes(storedKeyAddress);
            isDefined = true;
            if (size > growthThreshold) {
                growAndRehash();
            }
        }

        public void putNewKey(
                Object kvBaseObject,
                long kvBaseOffset) {
            putNewKey(kvBaseObject, kvBaseOffset, kvBaseObject, kvBaseOffset + KV_LENGTH_BYTES);
        }
    }

    /**
     * Allocate new data structures for this map. When calling this outside of the constructor,
     * make sure to keep references to the old data structures so that you can free them.
     *
     * @param capacity the new map capacity
     */
    private void allocate(int capacity) {
        capacity = Math.max((int) Math.min(Integer.MAX_VALUE, nextPowerOf2(capacity)), 64);
        longArray = new LongArray(memoryManager.allocate(capacity * 8 * 2));
        bitset = new BitSet(MemoryBlock.fromLongArray(new long[capacity / 64]));

        this.growthThreshold = (int) (capacity * loadFactor);
        this.mask = capacity - 1;
    }

    /**
     * Free all allocated memory associated with this map, including the storage for keys and values
     * as well as the hash map array itself.
     *
     * This method is idempotent.
     */
    public void free() {
        if (longArray != null) {
            memoryManager.free(longArray.memoryBlock());
            longArray = null;
        }
        if (bitset != null) {
            // The bitset's heap memory isn't managed by a memory manager, so no need to free it here.
            bitset = null;
        }
        Iterator<MemoryBlock> dataPagesIterator = dataPages.iterator();
        while (dataPagesIterator.hasNext()) {
            MemoryBlock block = dataPagesIterator.next();
            memoryManager.freePage(block);
            dataPagesIterator.remove();
        }
        assert(dataPages.isEmpty());
    }

    private void freeMeta() {
        if (longArray != null) {
            memoryManager.free(longArray.memoryBlock());
            longArray = null;
        }
        if (bitset != null) {
            // The bitset's heap memory isn't managed by a memory manager, so no need to free it here.
            bitset = null;
        }
        size = 0;
    }

    private long freePages() {
        long memoryFreed = 0l;
        Iterator<MemoryBlock> dataPagesIterator = dataPages.iterator();
        while (dataPagesIterator.hasNext()) {
            MemoryBlock block = dataPagesIterator.next();
            memoryFreed += block.size();
            memoryManager.freePage(block);
            shuffleMemoryManager.release(block.size());
            dataPagesIterator.remove();
        }

        dataPages.clear();
        currentDataPage = null;
        pageCursor = 0;
        return memoryFreed;
    }

    /** Returns the total amount of memory, in bytes, consumed by this map's managed structures. */
    public long getTotalMemoryConsumption() {
        return (
                dataPages.size() * PAGE_SIZE_BYTES +
                        bitset.memoryBlock().size() +
                        longArray.memoryBlock().size());
    }

    public long getMetaMemoryConsumption() {
        return (bitset.memoryBlock().size() +
                        longArray.memoryBlock().size());
    }

    /**
     * Returns the total amount of time spent resizing this map (in nanoseconds).
     */
    public long getTimeSpentResizingNs() {
        if (!enablePerfMetrics) {
            throw new IllegalStateException();
        }
        return timeSpentResizingNs;
    }


    /**
     * Returns the average number of probes per key lookup.
     */
    public double getAverageProbesPerLookup() {
        if (!enablePerfMetrics) {
            throw new IllegalStateException();
        }
        return (1.0 * numProbes) / numKeyLookups;
    }

    public long getNumHashCollisions() {
        if (!enablePerfMetrics) {
            throw new IllegalStateException();
        }
        return numHashCollisions;
    }

    /**
     * Grows the size of the hash table and re-hash everything.
     */
    private void growAndRehash() {
        long resizeStartTime = -1;
        if (enablePerfMetrics) {
            resizeStartTime = System.nanoTime();
        }
        // Store references to the old data structures to be used when we re-hash
        final LongArray oldLongArray = longArray;
        final BitSet oldBitSet = bitset;
        final int oldCapacity = (int) oldBitSet.capacity();

        // Allocate the new data structures
        allocate(Math.min(Integer.MAX_VALUE, growthStrategy.nextCapacity(oldCapacity)));

        // Re-mask (we don't recompute the hashcode because we stored all 32 bits of it)
        for (int pos = oldBitSet.nextSetBit(0); pos >= 0; pos = oldBitSet.nextSetBit(pos + 1)) {
            final long keyPointer = oldLongArray.get(pos * 2);
            final int hashcode = (int) oldLongArray.get(pos * 2 + 1);
            int newPos = hashcode & mask;
            int step = 1;
            boolean keepGoing = true;

            // No need to check for equality here when we insert so this has one less if branch than
            // the similar code path in addWithoutResize.
            while (keepGoing) {
                if (!bitset.isSet(newPos)) {
                    bitset.set(newPos);
                    longArray.set(newPos * 2, keyPointer);
                    longArray.set(newPos * 2 + 1, hashcode);
                    keepGoing = false;
                } else {
                    newPos = (newPos + step) & mask;
                    step++;
                }
            }
        }

        // Deallocate the old data structures.
        memoryManager.free(oldLongArray.memoryBlock());
        if (enablePerfMetrics) {
            timeSpentResizingNs += System.nanoTime() - resizeStartTime;
        }
    }

    /** Returns the next number greater or equal num that is power of 2. */
    private static long nextPowerOf2(long num) {
        final long highBit = Long.highestOneBit(num);
        return (highBit == num) ? num : highBit << 1;
    }





   // for sort

    private Sorter<PageRankPointer, LongArray> sorter;
    private static final class SortComparator implements Comparator<PageRankPointer> {
        @Override
        public int compare(PageRankPointer left, PageRankPointer right) {
            return left.partitionId - right.partitionId;
        }
    }
    private static final SortComparator SORT_COMPARATOR = new SortComparator();

    private void initializeForWriting() throws IOException {
        // TODO: move this sizing calculation logic into a static method of sorter:
        final long memoryRequested = initialSize * 8L * 2 + initialSize / 8L;
        final long memoryAcquired = shuffleMemoryManager.tryToAcquire(memoryRequested);
        if (memoryAcquired != memoryRequested) {
            shuffleMemoryManager.release(memoryAcquired);
            throw new IOException("Could not acquire " + memoryRequested + " bytes of memory");
        }
        allocate(initialSize);

        this.sorter = new Sorter<PageRankPointer, LongArray>(PageRankSortDataFormat.INSTANCE);
    }




    // for spill

    @VisibleForTesting
    static final int DISK_WRITE_BUFFER_SIZE = 1024 * 1024;

    /** The buffer size to use when writing spills using DiskBlockObjectWriter */
    private final int fileBufferSizeBytes;

    private final TaskContext taskContext;

    private final LinkedList<SpillInfo> spills = new LinkedList<SpillInfo>();

    public SpillInfo[] closeAndGetSpills() throws IOException {
        try {
            if (longArray != null) {
                // Do not count the final file towards the spill count.
                writeSortedFile(true);
                freePages();
            }
            return spills.toArray(new SpillInfo[spills.size()]);
        } catch (IOException e) {
            cleanupAfterError();
            throw e;
        }
    }

    /**
     * Sorts the in-memory records and writes the sorted records to an on-disk file.
     * This method does not free the sort data structures.
     *
     * @param isLastFile if true, this indicates that we're writing the final output file and that the
     *                   bytes written should be counted towards shuffle spill metrics rather than
     *                   shuffle write metrics.
     */
    private void writeSortedFile(boolean isLastFile) throws IOException {

        final ShuffleWriteMetrics writeMetricsToUse;

        if (isLastFile) {
            // We're writing the final non-spill file, so we _do_ want to count this as shuffle bytes.
            writeMetricsToUse = writeMetrics;
        } else {
            // We're spilling, so bytes written should be counted towards spill rather than write.
            // Create a dummy WriteMetrics object to absorb these metrics, since we don't want to count
            // them towards shuffle bytes written.
            writeMetricsToUse = new ShuffleWriteMetrics();
        }

        // This call performs the actual sort.
        sorter.sort(longArray, 0, size, SORT_COMPARATOR);

        // Currently, we need to open a new DiskBlockObjectWriter for each partition; we can avoid this
        // after SPARK-5581 is fixed.
        BlockObjectWriter writer;

        // Small writes to DiskBlockObjectWriter will be fairly inefficient. Since there doesn't seem to
        // be an API to directly transfer bytes from managed memory to the disk writer, we buffer
        // data through a byte array. This array does not need to be large enough to hold a single
        // record;
        final byte[] writeBuffer = new byte[DISK_WRITE_BUFFER_SIZE];

        // Because this output will be read during shuffle, its compression codec must be controlled by
        // spark.shuffle.compress instead of spark.shuffle.spill.compress, so we need to use
        // createTempShuffleBlock here; see SPARK-3426 for more details.
        final Tuple2<TempShuffleBlockId, File> spilledFileInfo =
                blockManager.diskBlockManager().createTempShuffleBlock();
        final File file = spilledFileInfo._2();
        final TempShuffleBlockId blockId = spilledFileInfo._1();
        final SpillInfo spillInfo = new SpillInfo(numPartitions, file, blockId);

        // Unfortunately, we need a serializer instance in order to construct a DiskBlockObjectWriter.
        // Our write path doesn't actually use this serializer (since we end up calling the `write()`
        // OutputStream methods), but DiskBlockObjectWriter still calls some methods on it. To work
        // around this, we pass a dummy no-op serializer.
        final DummySerializer ser = new DummySerializer();

        writer = blockManager.getDiskWriter(blockId, file, ser, fileBufferSizeBytes, writeMetricsToUse);

        int currentPartition = -1;
        for (int index = 0; index < size; index++) {
            final int partition = longArray.getHighHalf(index * 2 + 1);
            assert (partition >= currentPartition);
            if (partition != currentPartition) {
                // Switch to the new partition
                if (currentPartition != -1) {
                    writer.commitAndClose();
                    spillInfo.partitionLengths[currentPartition] = writer.fileSegment().length();
                }
                currentPartition = partition;
                writer =
                        blockManager.getDiskWriter(blockId, file, ser, fileBufferSizeBytes, writeMetricsToUse);
            }

            final long recordPointer = longArray.get(index * 2);
            final Object recordPage = memoryManager.getPage(recordPointer);
            final long recordOffsetInPage = memoryManager.getOffsetInPage(recordPointer);
            int dataRemaining = PlatformDependent.UNSAFE.getInt(recordPage, recordOffsetInPage);
            long recordReadPosition = recordOffsetInPage + 4; // skip over record length
            while (dataRemaining > 0) {
                final int toTransfer = Math.min(DISK_WRITE_BUFFER_SIZE, dataRemaining);
                PlatformDependent.copyMemory(
                        recordPage,
                        recordReadPosition,
                        writeBuffer,
                        PlatformDependent.BYTE_ARRAY_OFFSET,
                        toTransfer);
                writer.write(writeBuffer, 0, toTransfer);
                recordReadPosition += toTransfer;
                dataRemaining -= toTransfer;
            }
            writer.recordWritten();
        }

        if (writer != null) {
            writer.commitAndClose();
            // If `writeSortedFile()` was called from `closeAndGetSpills()` and no records were inserted,
            // then the file might be empty. Note that it might be better to avoid calling
            // writeSortedFile() in that case.
            if (currentPartition != -1) {
                spillInfo.partitionLengths[currentPartition] = writer.fileSegment().length();
                spills.add(spillInfo);
            }
        }

        if (!isLastFile) {  // i.e. this is a spill file
            // The current semantics of `shuffleRecordsWritten` seem to be that it's updated when records
            // are written to disk, not when they enter the shuffle sorting code. DiskBlockObjectWriter
            // relies on its `recordWritten()` method being called in order to trigger periodic updates to
            // `shuffleBytesWritten`. If we were to remove the `recordWritten()` call and increment that
            // counter at a higher-level, then the in-progress metrics for records written and bytes
            // written would get out of sync.
            //
            // When writing the last file, we pass `writeMetrics` directly to the DiskBlockObjectWriter;
            // in all other cases, we pass in a dummy write metrics to capture metrics, then copy those
            // metrics to the true write metrics here. The reason for performing this copying is so that
            // we can avoid reporting spilled bytes as shuffle write bytes.
            //
            // Note that we intentionally ignore the value of `writeMetricsToUse.shuffleWriteTime()`.
            // Consistent with ExternalSorter, we do not count this IO towards shuffle write time.
            // This means that this IO time is not accounted for anywhere; SPARK-3577 will fix this.
            writeMetrics.incShuffleRecordsWritten(writeMetricsToUse.shuffleRecordsWritten());
            taskContext.taskMetrics().incDiskBytesSpilled(writeMetricsToUse.shuffleBytesWritten());
        }
    }

    /**
     * Sort and spill the current records in response to memory pressure.
     */
    @VisibleForTesting
    void spill() throws IOException {
        logger.info("Thread {} spilling sort data of {} to disk ({} {} so far)",
                Thread.currentThread().getId(),
                Utils.bytesToString(getTotalMemoryConsumption()),
                spills.size(),
                spills.size() > 1 ? " times" : " time");

        writeSortedFile(false);
        final long metaMemoryUsage = getMetaMemoryConsumption();
        freeMeta();
        shuffleMemoryManager.release(metaMemoryUsage);
        final long spillSize = freePages();
        taskContext.taskMetrics().incMemoryBytesSpilled(spillSize);

        initializeForWriting();
    }

    /**
     * Force all memory and spill files to be deleted; called by shuffle error-handling code.
     */
    public void cleanupAfterError() {
        freePages();
        for (SpillInfo spill : spills) {
            if (spill.file.exists() && !spill.file.delete()) {
                logger.error("Unable to delete spill file {}", spill.file.getPath());
            }
        }
        if (longArray != null) {
            shuffleMemoryManager.release(getMetaMemoryConsumption());
            freeMeta();
        }
    }
}

