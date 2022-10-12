// Copyright 2022 The TuDB Authors. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.grapheco.tudb.test.index

import org.grapheco.tudb.store.index.{EmptyIndexServerImpl, IndexFactory, IndexServer, MemoryIndexServerImpl, RocksIndexServerImpl}
import org.junit._
import org.junit.runners.MethodSorters
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
class IndexServerTest {

  @Test
  def testCreateEngine(): Unit = {
    //empty index server
    val dummyIndexImpl = IndexFactory.newIndex("tudb://index?type=dummy")
    Assert.assertTrue(dummyIndexImpl.isInstanceOf[EmptyIndexServerImpl])
    val errorIndexImpl = IndexFactory.newIndex("asdfasdfasdf")
    Assert.assertTrue(errorIndexImpl.isInstanceOf[EmptyIndexServerImpl])
    val error2IndexImpl = IndexFactory.newIndex("tudb://index?type=asdfasdfasdf")
    Assert.assertTrue(error2IndexImpl.isInstanceOf[EmptyIndexServerImpl])
    val error3IndexImpl = IndexFactory.newIndex("https://www.google.com")
    Assert.assertTrue(error3IndexImpl.isInstanceOf[EmptyIndexServerImpl])
    val error4IndexImpl = IndexFactory.newIndex("tudb://www.google.com")
    Assert.assertTrue(error4IndexImpl.isInstanceOf[EmptyIndexServerImpl])
    val error5IndexImpl = IndexFactory.newIndex("asdfsdf://www.google.com")
    Assert.assertTrue(error5IndexImpl.isInstanceOf[EmptyIndexServerImpl])
    //memory index server
    val memoryIndexImpl = IndexFactory.newIndex("tudb://index?type=memory")
    Assert.assertTrue(memoryIndexImpl.isInstanceOf[MemoryIndexServerImpl])

    //rocksdb index server

    //test empty path
    val rocksDBIndexImpl = IndexFactory.newIndex("tudb://index?type=rocksdb&path=")
    Assert.assertTrue(rocksDBIndexImpl.isInstanceOf[EmptyIndexServerImpl])

    val rocksDB1IndexImpl = IndexFactory.newIndex("tudb://index?type=rocksdb&path=./test/index")
    Assert.assertTrue(rocksDB1IndexImpl.isInstanceOf[RocksIndexServerImpl])
  }

  @Test
  def testMemoryEngine(): Unit = {
    val memoryIndexImpl = IndexFactory.newIndex("tudb://index?type=memory")
    testEngineImpl(memoryIndexImpl)
  }

  @Test
  def testRocksDbEngine(): Unit = {

    val rocksdbIndexImpl = IndexFactory.newIndex("tudb://index?type=rocksdb&path=./test/RocksDB")
    testEngineImpl(rocksdbIndexImpl)
    rocksdbIndexImpl.close()
  }

  /*
  test index engine impl
   */
  def testEngineImpl(impl: IndexServer): Unit = {
    val (propertyKey, nodeId) = ("property_key_value", 1)
    impl.addIndex(propertyKey, nodeId)
    Assert.assertTrue(impl.getIndexByKey(propertyKey).contains(nodeId))
    Assert.assertFalse(impl.getIndexByKey(propertyKey).contains(2))
    impl.removeIndex(propertyKey, nodeId)
    Assert.assertFalse(impl.getIndexByKey(propertyKey).contains(nodeId))
    Assert.assertTrue(impl.getIndexByKey(propertyKey).isEmpty)
  }
  @Before
  def cleanUp(): Unit = {}

  @After
  def close(): Unit = {}

}
