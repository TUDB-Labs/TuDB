/** Copyright (c) 2022 TuDB * */
package org.grapheco.tudb.store.storage

import org.rocksdb._

/** @author: huagnlin
  * @createDate: 2022-06-28 7:19:08
  * @description: this is the rocksdb config class
  */
object RocksDBStorageConfig {

  /**
    * return default rocksdb config with lz4 compression
    * @param createIfMissing
    * @return
    */
  def getDefault(createIfMissing: Boolean): Options = {
    val options: Options = new Options()
    val tableConfig = new BlockBasedTableConfig()

    tableConfig
      .setFilterPolicy(new BloomFilter(15, false))
      .setBlockSize(16L * 1024L)
      .setBlockCache(new LRUCache(1024L * 1024L * 1024L))

    options
      .setTableFormatConfig(tableConfig)
      .setCreateIfMissing(createIfMissing)
      .setCompressionType(CompressionType.LZ4_COMPRESSION)
      .setCompactionStyle(CompactionStyle.LEVEL)
      .setDisableAutoCompactions(
        false
      ) // true will invalid the compaction trigger, maybe
      .setOptimizeFiltersForHits(
        false
      ) // true will not generate BloomFilter for L0.
      .setMaxBackgroundJobs(4)
      .setMaxWriteBufferNumber(8)
      .setSkipCheckingSstFileSizesOnDbOpen(true)
      .setLevelCompactionDynamicLevelBytes(true)
      .setAllowConcurrentMemtableWrite(true)
      .setCompactionReadaheadSize(2 * 1024 * 1024L)
      .setMaxOpenFiles(-1)
      .setUseDirectIoForFlushAndCompaction(true) // maybe importer use
      .setWriteBufferSize(256L * 1024L * 1024L)
      .setMinWriteBufferNumberToMerge(2) // 2 or 3 better performance
      .setLevel0FileNumCompactionTrigger(10) // level0 file num = 10, compression l0->l1 start.(invalid, why???);level0 size=256M * 3 * 10 = 7G
      .setLevel0SlowdownWritesTrigger(20)
      .setLevel0StopWritesTrigger(40)
      .setMaxBytesForLevelBase(256L * 1024L * 1024L) // total size of level1(same as level0)
      .setMaxBytesForLevelMultiplier(10)
      .setTargetFileSizeBase(
        256L * 1024L * 1024L
      ) // maxBytesForLevelBase / 10 or 15
  }

  /**
    * return high performance rocksdb config with no compression
    * @param createIfMissing
    * @return
    */
  def getHighPerformanceConfig(createIfMissing: Boolean): Options = {
    val options: Options = new Options()
    val tableConfig = new BlockBasedTableConfig()

    tableConfig
      .setFilterPolicy(new BloomFilter(10, false))
      .setBlockSize(16L * 1024L)
      .setBlockCache(new LRUCache(1024L * 1024L * 1024L))

    options
      .setTableFormatConfig(tableConfig)
      .setCreateIfMissing(createIfMissing)
      .setCompressionType(CompressionType.NO_COMPRESSION)
      .setCompactionStyle(CompactionStyle.LEVEL)
      .setDisableAutoCompactions(
        false
      ) // true will invalid the compaction trigger, maybe
      .setOptimizeFiltersForHits(
        false
      ) // true will not generate BloomFilter for L0.
      .setMaxBackgroundJobs(4)
      .setMaxWriteBufferNumber(8)
      .setSkipCheckingSstFileSizesOnDbOpen(true)
      .setLevelCompactionDynamicLevelBytes(true)
      .setAllowConcurrentMemtableWrite(true)
      .setCompactionReadaheadSize(2 * 1024 * 1024L)
      .setMaxOpenFiles(-1)
      .setUseDirectIoForFlushAndCompaction(true) // maybe importer use
      .setWriteBufferSize(256L * 1024L * 1024L)
      .setMinWriteBufferNumberToMerge(2) // 2 or 3 better performance
      .setLevel0FileNumCompactionTrigger(10) // level0 file num = 10, compression l0->l1 start.(invalid, why???);level0 size=256M * 3 * 10 = 7G
      .setLevel0SlowdownWritesTrigger(20)
      .setLevel0StopWritesTrigger(40)
      .setMaxBytesForLevelBase(
        256L * 1024L * 1024L * 2 * 10L
      ) // total size of level1(same as level0)
      .setMaxBytesForLevelMultiplier(10)
      .setTargetFileSizeBase(
        256L * 1024L * 1024L
      ) // maxBytesForLevelBase / 10 or 15
      .setTargetFileSizeMultiplier(2)
  }

}
