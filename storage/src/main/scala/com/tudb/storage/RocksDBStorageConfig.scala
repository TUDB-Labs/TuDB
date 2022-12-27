package com.tudb.storage

import org.rocksdb.{BlockBasedTableConfig, BloomFilter, CompactionStyle, CompressionType, LRUCache, Options}

/**
  *@description:
  */
object RocksDBStorageConfig {
  def getDefaultOptions(createIfMissing: Boolean): Options = {
    val options: Options = new Options()
    val tableConfig = new BlockBasedTableConfig()

    tableConfig
      .setFilterPolicy(new BloomFilter(10, false))
      .setBlockSize(32L * 1024L)
      .setBlockCache(new LRUCache(100L * 1024L * 1024L))

    options
      .setTableFormatConfig(tableConfig)
      .setCreateIfMissing(createIfMissing)
      .setCompressionType(CompressionType.LZ4_COMPRESSION)
      .setCompactionStyle(CompactionStyle.LEVEL)
      .setDisableAutoCompactions(false)
      .setOptimizeFiltersForHits(false)
      .setMaxBackgroundJobs(4)
      .setMaxWriteBufferNumber(8)
      .setSkipCheckingSstFileSizesOnDbOpen(true)
      .setLevelCompactionDynamicLevelBytes(true)
      .setAllowConcurrentMemtableWrite(true)
      .setMaxOpenFiles(51200)
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
