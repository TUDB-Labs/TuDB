package org.grapheco.tudb.test.index

import org.grapheco.tudb.store.index.{EmptyIndexServerImpl, IndexFactory, IndexServer, MemoryIndexServerImpl}
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
  }

  @Test
  def testMemoryEngine(): Unit = {
    val memoryIndexImpl = IndexFactory.newIndex("tudb://index?type=memory")
    memoryIndexImpl.init(Map.empty)
    testEngineImpl(memoryIndexImpl)
  }

  /*
  test index engine impl
   */
  def testEngineImpl(impl: IndexServer): Unit = {
    impl.addIndex("a", 1)
    Assert.assertTrue(impl.getIndexByKey("a").contains(1))
    Assert.assertFalse(impl.getIndexByKey("a").contains(2))
    impl.removeIndex("a", 1)
    Assert.assertFalse(impl.getIndexByKey("a").contains(1))
    Assert.assertTrue(impl.getIndexByKey("a").isEmpty)
  }


  @Before
  def cleanUp(): Unit = {

  }


}
