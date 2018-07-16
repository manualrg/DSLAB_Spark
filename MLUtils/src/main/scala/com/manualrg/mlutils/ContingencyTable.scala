package com.manualrg.mlutils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import breeze.linalg.{Vector => BV, DenseMatrix => BDM}

object ContingencyTable {


  val spark = SparkSession.builder().getOrCreate()
  import spark.implicits._

  case class ContingencyDataset(col_idx: Int, level_value: String, N1Col :Double, N0Col :Double)
  //ContingencyDatasets is a data structure that wraps contingency tables in a denormlized way in order to create a DataFrame
  case class ContingencyComputeRecord(col_idx: Array[Int], level_value: Array[String], N1Col: Array[Double], N0Col: Array[Double]) extends Serializable
  /*
   * ContintencyComputeRecord is a data structure that assembles the output of the contingency table calculation,
   * every contingency table goes in one instance of this case class:
   *  col_idx: Index of the column in the Array of features
   *  level_value: Values of the levels of every feature
   *  N1Col: Number of events (label=1) in level j of feature i
   *  N0Col: Number of non-events (label=1) in level j of feature i
   *  NCol: Total number of observations
   */


  /*
  * Transform a DataFrame into an RDD of pairs (Double, Array[String])
  * inputCols must be StringType and labelCol DoubleType
  * */
  def toRDD(dataset :DataFrame, inputCols :Array[String], labelCol :String) :org.apache.spark.rdd.RDD[(Double, Array[String])]= {
    val mlCols = (Array(labelCol) ++ inputCols).map(col(_))
    dataset.select(mlCols: _*).rdd.map(row => (row.toSeq.head.asInstanceOf[Double],
      row.toSeq.tail.toArray.map(_.asInstanceOf[String])))
  }

  /*
  * Contingency Table computation, takes as input:
  *   idx: feature index in feature Array
  *   features: For each feature j, a Map of its levels->index
  *   labels: A map of every label level, consider only binary classification problems
  * */
  def computeContingency(idx: Int, features: scala.collection.immutable.Map[String, Int], labels : scala.collection.immutable.Map[Double, Int],  contingency: breeze.linalg.DenseMatrix[Double]) :ContingencyComputeRecord = {
    import breeze.linalg._
    import breeze.math._
    import breeze.numerics._

    val event_idx =  labels(1.0)
    val nonevent_idx =  labels(0.0)

    val N0col :breeze.linalg.DenseVector[Double] = contingency(::, event_idx)
    val N1col :breeze.linalg.DenseVector[Double] = contingency(::, nonevent_idx)

    val level_value: Array[String] = features.keys.toArray
    val nLevels = features.size
    val col_idx: Array[Int] = Array.fill[Int](nLevels)(idx)
    //PropComputeRecord(col_idx, labels_idx, values)
    ContingencyComputeRecord(col_idx, level_value, N1col.toArray, N0col.toArray)
  }

  /*
  * Transform the input DataFrame
  * Compute PairCounts: Map(key -> count), where key is a tuple of three elements consisting in:
  *   (col_idx[Int], level_value[String], label_value[Double])
  *
  * Map labels: event supossed to be 1.0 and non event 0.0. Only Binary Classification
  *
  * Compute ContingencyTable: Build a matrix per feature. This contingency matrix will have as many columns as label values (2)
  * and as many rows as feature levels. Feature levels are mapped by features: Map[levels[String] -> level_idx[Int]]
  *
  * Get ContingencyTable for each feature as a DataFrame.
  * */
  def fit(dataset :DataFrame, inputCols :Array[String], labelCol :String) :DataFrame = {
    //Transform DF to RDD
    val rdd = toRDD(dataset, inputCols, labelCol)

    //get PairCounts
    val numCols = rdd.first()._2.size
    val results = new Array[ContingencyComputeRecord](numCols)
    var labels: Map[Double, Int] = null
    // at most 1000 columns at a time
    val batchSize = 100
    var batch = 0

    val startCol = batch * batchSize
    val endCol = startCol + math.min(batchSize, numCols - startCol)
    val pairMaps =  rdd.mapPartitions { iter =>
      val distinctLabels = scala.collection.mutable.HashSet.empty[Double]
      val allDistinctFeatures: Map[Int, scala.collection.mutable.HashSet[String]] =
        Map((startCol until endCol).map(col => (col, scala.collection.mutable.HashSet.empty[String])): _*)
      var i = 1 //contador de batches
      iter.flatMap { case (label, features) =>
        i += 1
        distinctLabels += label
        val brzFeatures = BV(features)
        (startCol until endCol).map { col =>
          val feature = brzFeatures(col)
          allDistinctFeatures(col) += feature
          (col, feature, label)
        }
      }//iter.flatMap {...}
    }//val pairMaps =  rdd.mapPartitions { iter =>

    //pairCounts: Map(iterable[(col_idx[Int], levels[String], label[Double])] -> count[Int])
    val pairCounts = pairMaps.countByValue()

    //map Labels
    if (labels == null) {
      // Do this only once for the first column since labels are invariant across features.
      labels = pairCounts.keys.filter(_._1 == startCol).map(_._3).toArray.distinct.sorted.zipWithIndex.toMap
    }
    val numLabels = labels.size
    //println("labels: " + labels)

    //get contingency Table
    pairCounts.keys.groupBy(_._1).foreach { case (col, keys) =>
      //keys: iterable[(col_idx[Int], levels[String], label[Double])]
      val features = keys.map(_._2).toArray.distinct.zipWithIndex.toMap
      //features: Map[levels[String] -> level_idx[Int]]
      //println("features: " + features)
      val numRows = features.size
      val contingency = new BDM(numRows, numLabels, new Array[Double](numRows * numLabels))
      keys.foreach { case (_, feature, label) =>
        val i = features(feature)
        val j = labels(label)
        contingency(i, j) += pairCounts((col, feature, label))
      }
      //results(col) = computeWOE(col, features, contingency)
      //At each iteration a contingency matrix is computed for each feature, rows are sorted by level_idx and columns by label_idx
      //println(contingency)
      val results_row = computeContingency(col, features, labels, contingency)
      results(col) = results_row
    }
    val contingencyTuples = results
    //spark.createDataFrame(propTuples).show()

    //get contingency table as a denormalized DataFrame
    getContingencyReport(contingencyTuples)
  }


  def getContingencyReport(contingency :Array[ContingencyComputeRecord]) :DataFrame ={
    //Flatten the results
    val col_idx_exp = contingency.flatMap(x => x.col_idx)
    val level_value_exp = contingency.flatMap(x => x.level_value)
    val N1Col_exp = contingency.flatMap(x => x.N1Col)
    val N0Col_exp = contingency.flatMap(x => x.N0Col)
    val min = List(col_idx_exp, level_value_exp, N1Col_exp, N0Col_exp).map(_.size).min
    val dataset = (0 until min) map { i => ContingencyDataset(col_idx_exp(i), level_value_exp(i), N1Col_exp(i), N0Col_exp(i)) }
    val contingency_df = spark.createDataFrame(dataset)
    contingency_df
  }
}
