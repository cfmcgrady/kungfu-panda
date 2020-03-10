package org.panda

/**
 * @time 2020/3/9 4:42 下午
 * @author fchen <cloud.chenfu@gmail.com>
 */
object Config {
  val CACHE_ROOT_DIR = sys.env.getOrElse("BAMBOO_CACHE_DIR", "/tmp/cache")
}
