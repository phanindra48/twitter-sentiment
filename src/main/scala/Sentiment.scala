object Sentiment extends Enumeration {
  type Sentiment = Value

  val NEUTRAL = Value("NEUTRAL")
  val POSITIVE = Value("POSITIVE")
  val NEGATIVE = Value("NEGATIVE")
  val STRONG_NEGATIVE = Value("STRONG_NEGATIVE")
  val STRONG_POSITIVE = Value("STRONG_POSITIVE")
  val WEAK_NEGATIVE = Value("WEAK_NEGATIVE")
  val WEAK_POSITIVE = Value("WEAK_POSITIVE")
  val NO_IDEA = Value("NO_IDEA")

  def toSentiment(sentiment: Int): Sentiment = sentiment match {
    case x if x < 0 || x >= 5 => Sentiment.NO_IDEA
    case 0 => Sentiment.WEAK_NEGATIVE
    case 1 => Sentiment.STRONG_NEGATIVE
    case 2 => Sentiment.NEUTRAL
    case 3 => Sentiment.WEAK_POSITIVE
    case 4 => Sentiment.STRONG_POSITIVE
  }
}
