package org.apache.spark.ml.regression

import org.apache.spark.SparkConf
import org.apache.spark.ml._
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.mllib.linalg.{Vector, Vectors, DenseVector}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable.SynchronizedQueue
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.{TypeTag, typeOf}


trait StreamToDataFrameFeeder {

  val transformQueue = new SynchronizedQueue[RDD[Double]]()

  def fit[A <: Product : TypeTag](dstream: DStream[A]): Unit = {
    feed(dstream, "fitStream", fitCallback)
  }

  def transform[A <: Product : TypeTag](dstream: DStream[A]): DStream[Double] = {
    val transformedStream = dstream.context.queueStream(transformQueue, oneAtATime = true)

    def transformThenQueue(dataset: DataFrame): Unit = {
      val transformedRDD = transformCallback(dataset)
      transformQueue += transformedRDD
      val result = transformedRDD.first()
      println(s"*** queued an rdd $result")
    }

    feed(dstream, "transformStream", transformThenQueue)

    //val fullPredictions = model.transform(data).cache()
    //val predictions = fullPredictions.select("prediction").map(_.getDouble(0))

    transformedStream
  }

  protected def fitCallback(dataset: DataFrame)

  protected def transformCallback(dataset: DataFrame): RDD[Double]

  private def feed[A <: Product : TypeTag](
      dstream: DStream[A],
      tableName: String,
      func: (DataFrame) => Unit): Unit = {

    dstream.foreachRDD { (rdd, t) =>

      if (rdd.isEmpty()) {
        println(s"*** $tableName RDD empty ${rdd.isEmpty()} at $t")
      }
      else {
        val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
        import sqlContext.implicits._

        //val df = rdd.map(x => LabeledPoint(1.0, Vectors.dense(1.0))).toDF()
        val df = rdd.toDF()
        df.registerTempTable(tableName)

        println(s"*** func $tableName at $t")

        func(df)
      }
    }
  }
}

class StreamingLinearRegression extends LinearRegression with StreamToDataFrameFeeder {

  protected var model: Option[LinearRegressionModel] = None

  def latestModel: LinearRegressionModel = {
    model.get
  }

  protected def fitCallback(dataset: DataFrame): Unit = {
    fit(dataset)
  }

  protected def transformCallback(dataset: DataFrame): RDD[Double] = {
    println(s"*** Model defined ${model.isDefined}")
    Thread.dumpStack()
    if (model.isDefined) model.get.transform(dataset).select("prediction").map(_.getDouble(0))
    else dataset.rdd.map(x => 0.0)
  }

  override protected def train(dataFrame: DataFrame): LinearRegressionModel = {

    // TODO - need to set initial model to previous before train
    //model.foreach(m => lir.setInitialModel)

    val updatedModel = super.train(dataFrame)
    model = Some(updatedModel)
    println("*** Model updated")
    updatedModel
  }

  // TODO - not allow DataFrame fit ???
  /*override protected def fit(dataset: DataFrame): LinearRegressionModel ={
    super.fit(dataset)
  }*/
}

class StreamingPipeline(pipeline: Pipeline) extends StreamToDataFrameFeeder {

  protected var model: Option[PipelineModel] = None

  protected def fitCallback(dataset: DataFrame): Unit = {
    model = Some(pipeline.fit(dataset))
  }

  protected def transformCallback(dataset: DataFrame): RDD[Double] = {
    model.get.transform(dataset).select("prediction").map(_.getDouble(0))
  }

}

/*
class Test {

  def run(): Unit ={

    val conf = new SparkConf().setMaster("local").setAppName("StreamingLinearRegression")
    val ssc = new StreamingContext(conf, Seconds(1))

    val trainingData = ssc.textFileStream("balh").map(LabeledPoint.parse)
    val testData = ssc.textFileStream("balh").map(LabeledPoint.parse)

    val slr = new StreamingLinearRegression()
    slr.fit(trainingData)
    val results1 = slr.transform(testData)

    val pl = new Pipeline().setStages(Array(slr))
    val splf = new StreamingPipeline(pl)
    splf.fit(trainingData)
    val results2= splf.transform(testData)



  }
}*/

/*
class StreamingLinearRegression(override val uid: String)
extends PipelineStage {
    //extends Regressor[Vector, LinearRegression, LinearRegressionModel] {

  def this() = this(Identifiable.randomUID("StrmLinReg"))

  override def copy(extra: ParamMap): StreamingLinearRegression = defaultCopy(extra)
}

class StreamingLinearRegression*/

/*
val slir = StreamingLinearRegression() // regressor < predictor < estimator < pipelineStage

val slirModel = slir.fit(trainingData)     // regressionModel < predictionModel < model < transformer < pipelineStage

val fullPredictions = slirModel.transform(testData)
*/


/*
class StreamInputStage[T: ClassTag] {

  def dstreamToDataFrame(dstream: DStream[T], (df: DataFrame) => ): Unit = {

    dstream.foreachRDD { rdd =>
      val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
      import sqlContext.implicits._

      val df = rdd.map(x => x.toString).map(w => Record(w)).toDF()
      df.registerTempTable("words")

    }

  }
}*/

/*
abstract class EstimatorUpdater[M <: Model[M]](e: Estimator[M]) extends Estimator {

  override def fit(dataset: DataFrame): M = {
    val curr = e.fit(dataset)
    updateModel(curr)
    curr
  }

  override def transformSchema(schema: StructType): StructType = {
    e.transformSchema(schema)
  }

  def updateModel(m: M)
}*/

/*
class LinearRegressionUpdater(lir: LinearRegression) extends Predictor[Vector, LinearRegression, LinearRegressionModel] {//EstimatorUpdater[LinearRegressionModel](lir) {

  protected var model: Option[LinearRegressionModel] = None

  def train(dataset: DataFrame): LinearRegressionModel = {

    // TODO - need to set initial model to previous before train
    //model.foreach(m => lir.setInitialModel)

    val curr = lir.fit(dataset)
    model = Some(curr)
    curr
  }
}*/