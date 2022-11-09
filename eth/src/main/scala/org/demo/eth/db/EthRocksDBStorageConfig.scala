package org.demo.eth.db

import org.rocksdb._

/**
  *@description:
  */
object EthRocksDBStorageConfig {

  /**
    * return default rocksdb config with lz4 compression
    * @param createIfMissing
    * @return
    */
  def getDefault(createIfMissing: Boolean): Options = {
    val options: Options = new Options()
    val tableConfig = new BlockBasedTableConfig()

    tableConfig
      .setFilterPolicy(new BloomFilter(10, false))
      .setBlockSize(32L * 1024L)
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
      .setMaxOpenFiles(-1)
      .setUseDirectIoForFlushAndCompaction(true) // maybe importer use
      .setWriteBufferSize(64L * 1024L * 1024L)
      .setMinWriteBufferNumberToMerge(2) // 2 or 3 better performance
      .setLevel0FileNumCompactionTrigger(10)
      .setLevel0SlowdownWritesTrigger(20)
      .setLevel0StopWritesTrigger(40)
      .setMaxBytesForLevelBase(256L * 1024L * 1024L) // total size of level1(same as level0)
      .setMaxBytesForLevelMultiplier(10)
      .setTargetFileSizeBase(
        64L * 1024L * 1024L
      ) // maxBytesForLevelBase / 10 or 15
  }

}
