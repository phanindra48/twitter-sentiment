import org.apache.spark.ml.feature._
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}

import org.apache.spark.ml.{Pipeline}
import org.apache.spark.ml.classification._
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.{Row, SparkSession}

object AirlineSentiment {

  /** Makes sure only ERROR messages get logged to avoid log spam. */
  def setupLogging() = {
    import org.apache.log4j.{Level, Logger}
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
  }

  def generateCVForLR(tokenizer: Tokenizer, remover: StopWordsRemover, hashingTF: HashingTF, indexer: StringIndexer): CrossValidator = {
    val algorithm = "Logistic Regression"
    val lr = new LogisticRegression()

    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.elasticNetParam, Array(.4, .8))
      .addGrid(lr.maxIter, Array(10, 30, 60))

    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, remover, hashingTF, indexer, lr))

    // We now treat the Pipeline as an Estimator, wrapping it in a CrossValidator instance.
    // This will allow us to jointly choose parameters for all Pipeline stages.
    // A CrossValidator requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
    val crossVal = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new MulticlassClassificationEvaluator())
      .setEstimatorParamMaps(paramGrid.build())
      .setNumFolds(3)
    return crossVal
  }

  def generateCVForNB(tokenizer: Tokenizer, remover: StopWordsRemover, hashingTF: HashingTF, indexer: StringIndexer): CrossValidator = {
    val algorithm = "Naive Bayes"
    val nb = new NaiveBayes()

    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, remover, hashingTF, indexer, nb))

    val paramGrid = new ParamGridBuilder()

    // We now treat the Pipeline as an Estimator, wrapping it in a CrossValidator instance.
    // This will allow us to jointly choose parameters for all Pipeline stages.
    // A CrossValidator requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
    val crossVal = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new MulticlassClassificationEvaluator())
      .setEstimatorParamMaps(paramGrid.build())
      .setNumFolds(3)
    return crossVal
  }

  def main(args: Array[String]): Unit = {
    //val spark = new SparkConf().setAppName("AirlineTweets").setMaster("local[*]")

    val spark = SparkSession.builder
      //.master("local")
      .appName("Airline Tweets Sentiment Analysis")
      .getOrCreate

    import spark.implicits._

    if (args.length < 2) {
      System.err.println("Correct usage: Program_Name inputPath outputPath")
      System.exit(1)
    }

    // Get rid of log spam (should be called after the context is set up)
    setupLogging()

    val inputPath = args(0)
    val outputPath = args(1)
    println("inputPath: " + inputPath)
    println("outputPath: " + outputPath)

    println("Loading input file")
    val rawDF = spark.read
      .format("csv")
      .option("header", "true") // first line in file has headers
      .option("mode", "DROPMALFORMED")
      .load(inputPath)

    val tweetsDF = rawDF.filter("text is not null")


    val labelCol = "label"
    val predictionCol = "prediction"
    val metric = "accuracy"
    val trainPer = 0.8


    /* Preprocessing steps */
    // 1. tokenizer
    println("Running Tokenizer")
    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")

    // 2. Stopwords removal
    println("Running StopwordRemover")
    val stopWordRemover = new StopWordsRemover().setInputCol(tokenizer.getOutputCol).setOutputCol("filtered")

    // 3. Hashing Term Frequency
    println("Running HashingTF")
    val hashingTF = new HashingTF().setInputCol(stopWordRemover.getOutputCol).setOutputCol("features")

    // 4. Encode sentiment column
    println("Running Indexer")
    val strIndexer = new StringIndexer().setInputCol("airline_sentiment").setOutputCol(labelCol)

    // algorithm
    println("Generating CV")
    val crossVal = generateCVForLR(tokenizer, stopWordRemover, hashingTF, strIndexer)
    //val crossVal = generateCVForNB(tokenizer, stopWordRemover, hashingTF, strIndexer)

    val Array(trainingDF, testDF) = tweetsDF.randomSplit(Array(trainPer, (1 - trainPer)))

    // Run cross-validation, and choose the best set of parameters.
    println("Generating Model")
    val model = crossVal.fit(trainingDF)

    val result = model.transform(testDF)
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol(labelCol)
      .setPredictionCol(predictionCol)
      .setMetricName(metric)

    val accuracy = evaluator.evaluate(result)
    println("Classifier: Naive Bayes")
    println(" Test error: " + (1 - accuracy))

    result.select("text", "airline_sentiment",  labelCol, predictionCol).show(100)

    // Compute raw scores on the test set
    // Instantiate metrics object

    val predictionAndLabels = result.select("label", "prediction").map {
      case Row(label: Double, prediction: Double) => (prediction, label)
    }

    val metrics = new MulticlassMetrics(predictionAndLabels.rdd)

    // Precision by label
    val labels = metrics.labels
    val precisionByLabel = labels.map { l =>
      s"Precision($l) = " + metrics.precision(l)
    }.mkString("\n")

    // Recall by label
    val recall = labels.map { l =>
      s"Recall($l) = " + metrics.recall(l)
    }.mkString("\n")

    // False positive rate by label
    val fprByLabel = labels.map { l =>
      s"FPR($l) = " + metrics.falsePositiveRate(l)
    }.mkString("\n")

    // F-measure by label
    val fmeasure = labels.map { l =>
      s"F1-Score($l) = " + metrics.fMeasure(l)
    }.mkString("/n")

    val F1Scores = labels.map { l =>
      s"F1-Score(${l}) = " + metrics.fMeasure(l)
    }.mkString("\n")


    val metricsStr =
      s"""
         |Confusion Matrix
         |${metrics.confusionMatrix}

         |Overall Statistics
         |Summary Statistics
         |Accuracy = ${metrics.accuracy}

         |Precision by label
         |${precisionByLabel}
         |Recall by label
         |${recall}
         |False positive rate by label
         |${fprByLabel}

         |F-measure by label
         |${fmeasure}

         |${F1Scores}

         |Weighted Stats
         |Weighted precision: ${metrics.weightedPrecision}
         |Weighted recall: ${metrics.weightedRecall}
         |Weighted F1 score: ${metrics.weightedFMeasure}
         |Weighted false positive rate: ${metrics.weightedFalsePositiveRate}
      """.stripMargin
    println(metricsStr)

    // Save output to file
    println("Saving output to file: " + outputPath)

    spark.sparkContext.parallelize(List(metricsStr)).coalesce(1, shuffle = true).saveAsTextFile(outputPath)
  }
}

