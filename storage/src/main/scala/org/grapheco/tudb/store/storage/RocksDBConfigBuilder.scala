package org.grapheco.tudb.store.storage

import com.typesafe.scalalogging.LazyLogging
import org.grapheco.tudb.exception.TuDBException
import org.rocksdb._

import java.io.{BufferedInputStream, File, FileInputStream}
import java.util.Properties
import scala.collection.JavaConverters._

/** @Author: Airzihao
  * @Description:
  * @Date: Created at 9:37 下午 2022/1/31
  * @Modified By:
  */
class RocksDBConfigBuilder(rocksFile: File) extends LazyLogging {
  implicit def strToBoolean(str: String) = str.toBoolean

  def getOptions(): Options = {
    val prop = new Properties()
    val is = new BufferedInputStream(new FileInputStream(rocksFile))
    prop.load(is)
    val settings = prop.asScala
    logger.debug(s"settings nums: ${settings.size}")

    val options: Options = new Options()
    val tableConfig = new BlockBasedTableConfig()
    var bloomFilterBits = 10

    val keys = prop.keySet().asScala.map(x => x.toString)

    keys.foreach {
      case "tableConfig.setBlockSize" => {
        tableConfig.setBlockSize(
          prop.getProperty("tableConfig.setBlockSize").toLong
        )
      }
      case "tableConfig.setBlockCache" => {
        val num = prop.getProperty("tableConfig.setBlockCache").toLong
        tableConfig.setBlockCache(new LRUCache(num))
      }
      case "tableConfig.setCacheIndexAndFilterBlocks" => {
        tableConfig.setCacheIndexAndFilterBlocks(
          prop.getProperty("tableConfig.setCacheIndexAndFilterBlocks")
        )
      }
      case "tableConfig.setPinL0FilterAndIndexBlocksInCache" => {
        tableConfig.setPinL0FilterAndIndexBlocksInCache(
          prop.getProperty("tableConfig.setPinL0FilterAndIndexBlocksInCache")
        )
      }
      case "tableConfig.bloomFilterBits" => {
        bloomFilterBits = prop.getProperty("tableConfig.bloomFilterBits").toInt
      }
      ///////////////////
      case "options.setCreateIfMissing" => {
        options.setCreateIfMissing(
          prop.getProperty("options.setCreateIfMissing")
        )
      }
      case "options.setCompressionType" => {
        val compressionType = {
          val str = prop.getProperty("options.setCompressionType")
          str match {
            case "BZLIB2_COMPRESSION" => CompressionType.BZLIB2_COMPRESSION
            case "DISABLE_COMPRESSION_OPTION" =>
              CompressionType.DISABLE_COMPRESSION_OPTION
            case "LZ4_COMPRESSION"    => CompressionType.LZ4_COMPRESSION
            case "LZ4HC_COMPRESSION"  => CompressionType.LZ4HC_COMPRESSION
            case "NO_COMPRESSION"     => CompressionType.NO_COMPRESSION
            case "SNAPPY_COMPRESSION" => CompressionType.SNAPPY_COMPRESSION
            case "XPRESS_COMPRESSION" => CompressionType.XPRESS_COMPRESSION
            case "ZLIB_COMPRESSION"   => CompressionType.ZLIB_COMPRESSION
            case "ZSTD_COMPRESSION"   => CompressionType.ZSTD_COMPRESSION
            case s =>
              throw new TuDBException(s"not support $s config settings")
          }
        }
        options.setCompressionType(compressionType)
      }
      case "options.setCompactionStyle" => {
        val str = prop.getProperty("options.setCompactionStyle")
        str match {
          case "LEVEL" => options.setCompactionStyle(CompactionStyle.LEVEL)
          case "FIFO"  => options.setCompactionStyle(CompactionStyle.FIFO)
          case "NONE"  => options.setCompactionStyle(CompactionStyle.NONE)
          case "UNIVERSAL" =>
            options.setCompactionStyle(CompactionStyle.UNIVERSAL)
          case _ => options.setCompactionStyle(CompactionStyle.LEVEL)
        }
      }
      case "options.setOptimizeFiltersForHits()" => {
        options.setOptimizeFiltersForHits(
          prop.getProperty("options.setOptimizeFiltersForHits()")
        )
      }
      case "options.setDisableAutoCompactions" => {
        options.setDisableAutoCompactions(
          prop.getProperty("options.setDisableAutoCompactions")
        )
      }
      case "options.setOptimizeFiltersForHits" => {
        options.setOptimizeFiltersForHits(
          prop.getProperty("options.setOptimizeFiltersForHits")
        )
      }
      case "options.setSkipCheckingSstFileSizesOnDbOpen" => {
        options.setSkipCheckingSstFileSizesOnDbOpen(
          prop.getProperty("options.setSkipCheckingSstFileSizesOnDbOpen")
        )
      }
      case "options.setLevelCompactionDynamicLevelBytes" => {
        options.setLevelCompactionDynamicLevelBytes(
          prop.getProperty("options.setLevelCompactionDynamicLevelBytes")
        )
      }
      case "options.setAllowConcurrentMemtableWrite" => {
        options.setAllowConcurrentMemtableWrite(
          prop.getProperty("options.setAllowConcurrentMemtableWrite")
        )
      }
      case "options.setMaxOpenFiles" => {
        options.setMaxOpenFiles(
          prop.getProperty("options.setMaxOpenFiles").toInt
        )
      }
      case "options.setCompactionReadaheadSize" => {
        options.setCompactionReadaheadSize(
          prop.getProperty("options.setCompactionReadaheadSize").toLong
        )
      }
      case "options.setWriteBufferSize" => {
        options.setWriteBufferSize(
          prop.getProperty("options.setWriteBufferSize").toLong
        )
      }
      case "options.setMinWriteBufferNumberToMerge" => {
        options.setMinWriteBufferNumberToMerge(
          prop.getProperty("options.setMinWriteBufferNumberToMerge").toInt
        )
      }
      case "options.setMaxBackgroundJobs" => {
        options.setMaxBackgroundJobs(
          prop.getProperty("options.setMaxBackgroundJobs").toInt
        )
      }
      case "options.setMaxFileOpeningThreads" => {
        options.setMaxFileOpeningThreads(
          prop.getProperty("options.setMaxFileOpeningThreads").toInt
        )
      }
      case "options.setMaxWriteBufferNumber" => {
        options.setMaxWriteBufferNumber(
          prop.getProperty("options.setMaxWriteBufferNumber").toInt
        )
      }
      case "options.setLevel0FileNumCompactionTrigger" => {
        options.setLevel0FileNumCompactionTrigger(
          prop.getProperty("options.setLevel0FileNumCompactionTrigger").toInt
        )
      }
      case "options.setLevel0SlowdownWritesTrigger" => {
        options.setLevel0SlowdownWritesTrigger(
          prop.getProperty("options.setLevel0SlowdownWritesTrigger").toInt
        )
      }
      case "options.setLevel0StopWritesTrigger" => {
        options.setLevel0StopWritesTrigger(
          prop.getProperty("options.setLevel0StopWritesTrigger").toInt
        )
      }
      case "options.setMaxBytesForLevelBase" => {
        options.setMaxBytesForLevelBase(
          prop.getProperty("options.setMaxBytesForLevelBase").toLong
        )
      }
      case "options.setMaxBytesForLevelMultiplier" => {
        options.setMaxBytesForLevelMultiplier(
          prop.getProperty("options.setMaxBytesForLevelMultiplier").toInt
        )
      }
      case "options.setTargetFileSizeBase" => {
        options.setTargetFileSizeBase(
          prop.getProperty("options.setTargetFileSizeBase").toLong
        )
      }
      case "options.setTargetFileSizeMultiplier" => {
        options.setTargetFileSizeMultiplier(
          prop.getProperty("options.setTargetFileSizeMultiplier").toInt
        )
      }

      case k => {
        throw new TuDBException(s"not support $k config setting")
      }
    }
    tableConfig.setFilterPolicy(new BloomFilter(bloomFilterBits, false))
    options.setTableFormatConfig(tableConfig)
    options
  }

}
