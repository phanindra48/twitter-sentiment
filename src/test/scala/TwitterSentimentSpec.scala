import org.scalatest.{FunSpec, Matchers}

class TwitterSentimentSpec extends FunSpec with Matchers {

  describe("sentiment analyzer") {

    it("should return POSITIVE when input has positive emotion") {
      val input = "Scala is a great general purpose language."
      val (_, sentiment) = TwitterSentiment.maxLengthSentiment(input)
      sentiment should be(Sentiment.WEAK_POSITIVE)
    }

    it("should return NEGATIVE when input has negative emotion") {
      val input = "Dhoni laments bowling, fielding errors in series loss"
      val (_, sentiment) = TwitterSentiment.maxLengthSentiment(input)
      sentiment should be(Sentiment.STRONG_NEGATIVE)
    }

    it("should return NEUTRAL when input has no emotion") {
      val input = "I am reading a book"
      val (_, sentiment) = TwitterSentiment.maxLengthSentiment(input)
      sentiment should be(Sentiment.NEUTRAL)
    }
  }
}