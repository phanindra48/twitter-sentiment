import org.scalatest.{FunSpec, Matchers}

class TwitterSentimentSpec extends FunSpec with Matchers {

  describe("sentiment analyzer") {

    it("should return POSITIVE when input has positive emotion") {
      val input = "Scala is a great general purpose language."
      val sentiment = TwitterSentiment.maxLengthSentiment(input)
      sentiment should be(Sentiment.POSITIVE)
    }

    it("should return NEGATIVE when input has negative emotion") {
      val input = "Dhoni laments bowling, fielding errors in series loss"
      val sentiment = TwitterSentiment.maxLengthSentiment(input)
      sentiment should be(Sentiment.NEGATIVE)
    }

    it("should return NEUTRAL when input has no emotion") {
      val input = "I am reading a book"
      val sentiment = TwitterSentiment.maxLengthSentiment(input)
      sentiment should be(Sentiment.NEUTRAL)
    }
  }
}