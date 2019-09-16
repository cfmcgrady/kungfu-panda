package org.panda.bamboo.util

import scala.collection.JavaConverters._

import org.scalatest.FunSuite

/**
 * @time 2019-09-16 11:45
 * @author fchen <cloud.chenfu@gmail.com>
 */
class CacheKeySuite extends FunSuite {
  test("key with different class type should not equals the same.") {
    val k1 = CacheKey("a", Map.empty[String, Object].asJava)
    val k2 = CacheKey("a", Map.empty[String, Object].asJava)
    val k3 = MLFlowRunCacheKey("a")
    assert(k1 == k2)
    assert(k1.hashCode() == k3.hashCode())
    assert(k1.equals(k2))
    assert(!k1.equals(k3))
  }

}
