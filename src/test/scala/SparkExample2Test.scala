/**
  * Created by Tam.Laniado on 30/10/2016.
  */

import scalaexamples.SparkExample2
import org.scalatest._



class SparkExample2Test extends FlatSpec with Matchers {
  "SparkExample2" should "return successful example of text matches" in {

      val expectedNumMatches  = 12
      val sparkExample2 = SparkExample2;
      val numTextMatches  = sparkExample2.doAnalysis("and")

      assert(numTextMatches == expectedNumMatches)

//    true should be === true
  }


  "SparkExample2" should "return failed example of text matches" in {

    val expectedNumMatches  = 13
    val sparkExample2 = SparkExample2;
    val numTextMatches  = sparkExample2.doAnalysis("and")

    assert(numTextMatches == expectedNumMatches)

    //    true should be === true
  }

}