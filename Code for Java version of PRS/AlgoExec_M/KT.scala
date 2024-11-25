package AlgoExec_M

import AlgoExec_W.KTWithinCore
import org.apache.spark.{HashPartitioner, SparkContext}
import SampleGen.SampleGenerate

import scala.collection.JavaConverters._
import scala.util.Random

/**
 * KT object to perform KT selection algorithm on alternatives.
 *
 *  ALT A list of SampleGenerate objects representing alternatives.
 *  params An array of doubles representing the parameters for KTPlus algorithm.
 *
 * Command line arguments:
 *  alpha Significance level for the KTPlus algorithm.
 *  delta Indifference zone parameter.
 *  n0 Initial sample size for each alternative.
 *  g Number of groups for partitioning.
 *  nCores Number of CPU cores to use.
 *  inputPath Path to the input data file.
 *  nAlt (Optional) Number of alternatives.
 *  seed (Optional) Seed for random number generation.
 *  repeat (Optional) Number of repetitions for the experiment.
 */
object KT {

  /**
   * Function to select the best alternative using KTPlus algorithm
   *
   * @param ALT List of SampleGenerate objects representing alternatives
   * @param params Array of parameters for KTPlus algorithm
   * @return Best SampleGenerate alternative
   */
  private def select(ALT: List[SampleGenerate], params: Array[Double]): SampleGenerate = {
    val selection = new KTWithinCore(ALT.asJava, params) // Create a KTPlus instance with alternatives and parameters
    selection.KTSelect() // Perform KT selection
    selection.getBestAlt // Return the best alternative
  }

  /**
   * Function to generate a sample using given parameters
   *
   * @param params Array of parameters for SampleGenerate
   * @return SampleGenerate instance
   */
  private def sampleGeneration(params: Array[Double]): SampleGenerate = {
    new SampleGenerate(params) // Create a new SampleGenerate instance
  }

  /**
   * Function to generate samples with or without seed
   *
   * @param sampleParam RDD of input data lines
   * @param seed Seed for random number generation
   * @return RDD of (ID, SampleGenerate) pairs
   */
  // Function to generate samples with or without seed
  private def generateSample(sampleParam: org.apache.spark.rdd.RDD[String], seed: Int): org.apache.spark.rdd.RDD[(Int, SampleGenerate)] = {
    // Map over each line in the RDD and generate samples with a seeded random number generator
    sampleParam.map { line =>
      val (id, param) = parseLine(line) // Parse the line into an ID and parameters
      val random = new Random(seed + id) // Initialize a new random generator with a seed unique to the ID
      val altSeed = random.nextInt() // Generate an alternative seed using the random generator
      val alt = sampleGeneration(param) // Generate a sample using the parameters
      alt.setSeed(altSeed) // Set the seed for the generated sample to ensure reproducibility
      (id, alt) // Return a tuple containing the ID and the generated sample
    }
  }

  /**
   * Function to parse a line of input into an ID and parameters array.
   * This function splits a line of input text into its components, converting the values into the appropriate types.
   *
   * @param line A line of input text.
   * @return A tuple containing the ID (as an Int) and the parameters (as an Array[Double]).
   */
  private def parseLine(line: String): (Int, Array[Double]) = {
    val param = line.split(" ") // Split line by spaces
    val id = param(0).toInt // The first element is the ID
    val params = param.map(_.toDouble)
    (id, params) // Return tuple of ID and parameters
  }

  /**
   * Function to run the KT algorithm
   *
   * @param sc SparkContext
   * @param paramOfSelect Parameters for KTPlus algorithm
   * @param nCores Number of CPU cores
   * @param inputPath Input data file path
   * @param seed Seed for random number generation
   * @param repeat Number of repetitions
   */
  def runKT(sc: SparkContext, paramOfSelect: Array[Double], nCores: Int, inputPath: String, seed: Int, repeat: Int): Seq[(Int, Int, Double, Double)] = {
    val results = new scala.collection.mutable.ArrayBuffer[(Int, Int, Double, Double)]() // Buffer to store results
    var count = 0 // Initialize repetition counter
    val randomSeedGenerator = new Random(seed) // Initialize random seed

    // Repeat the KT algorithm for the specified number of times
    while (count < repeat) {
      val startTime = System.nanoTime() // Record start time
      val repeatSeed = randomSeedGenerator.nextInt() // Generate common random number

      // Read input data and generate samples
      val sampleParam = sc.textFile(inputPath, nCores) // Read input data file
      val sample = generateSample(sampleParam, repeatSeed) // Generate samples

      // Partition the samples
      val uniformSampleKey = sample.partitionBy(new HashPartitioner(nCores)) // Partition samples
      val uniformSample = uniformSampleKey.map(_._2).glom().map(_.toList)

      // Perform the KT selection
      val bestAltCore = uniformSample.map(ALT => select(ALT, paramOfSelect)) // Map to select best alternative

      // Reduce to find the best alternative
      val bestAlt = bestAltCore.reduce { (x, y) =>
        if (x.getMean > y.getMean) { // Compare means
          x.setN(x.getN + y.getN) // Update sample size
          x.setSimulateTime(x.getSimulateTime + y.getSimulateTime) // Update simulation time
          x // Return x if its mean is higher
        } else {
          y.setN(y.getN + x.getN) // Update sample size
          y.setSimulateTime(y.getSimulateTime + x.getSimulateTime) // Update simulation time
          y // Return y if its mean is higher
        }
      }

      // Collect results
      val replication = count+1
      val bestID = bestAlt.getId // Get ID of best alternative
      val wallClockTime = (System.nanoTime() - startTime) / 1e9 // Calculate wall clock time
      val totalSimulateTime = bestAlt.getSimulateTime // Get total simulation time

      results += ((replication, bestID, wallClockTime, totalSimulateTime)) // Add results to buffer
      count += 1 // Increment counter
    }
    // Return the results as a sequence
    results
  }
}