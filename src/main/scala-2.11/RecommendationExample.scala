// Based upon Apache Spark documentation:
// https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/mllib/RecommendationExample.scala

package recommendationexample

import org.apache.log4j.Logger
import org.apache.log4j.Level

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating

object RecommendationExample {
  def main(args: Array[String]): Unit = {
    // cleanup console details, only show warnings and above
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    // Spark uses some hadoop libraries - binaries stored in D:\hadoop\bin, local processing only
    System.setProperty("hadoop.home.dir", "D:\\hadoop\\")
    val conf = new SparkConf().setAppName("RecommendationExample").setMaster("local")

    val sc = new SparkContext(conf)
    // Load and parse the data from figure 9.8
    val data = sc.textFile("test.data")
    val ratings = data.map(_.split(',') match { case Array(user, item, rate) =>
      Rating(user.toInt, item.toInt, rate.toDouble)
    })

    val rank = 6 // number of latent factors in the model
    val numIterations = 10 // number of iterations run

    // ** Build the recommendation model using ALS - alternating least-squares
    val model = ALS.train(ratings, rank, numIterations, 0.1)

    // Create map of explicit test data
    val usersProducts = ratings.map { case Rating(user, product, rate) =>
      (user, product)
    }

    // create ratings predictions
    val predictions =
      model.predict(usersProducts).map { case Rating(user, product, rate) =>
        println("Predictions: " + ((user, product), rate))
        ((user, product), rate)
      }

    val ratesAndPreds = ratings.map { case Rating(user, product, rate) =>
      println("Combine rates and predictions: " + ((user, product), rate))
      ((user, product), rate)
    }.join(predictions)

    // calculate error between prediction and explicit value, then calculate MSE
    val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
      val err = r1 - r2
      println("Calculated error: "  + user + " " + product + " " + err)

      err * err
    }.mean()
    println("Mean Squared Error = " + MSE)

  }
}