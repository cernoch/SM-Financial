package cernoch.sm.financial

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SqlTest extends Specification {

  "Financial dataset" should {
    "read all records" in {
      Financial.dump.size must_== 241 * 128 * 7 * 5
    }
  }
}