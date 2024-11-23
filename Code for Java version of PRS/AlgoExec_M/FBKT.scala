package AlgoExec_M

import AlgoExec_W.FBKTWithinCore
import org.apache.spark.{HashPartitioner, SparkContext}
import SampleGen.SampleGenerate

import scala.collection.JavaConverters._
import scala.util.Random

object FBKT {
  /**
   * Function to select the best alternative using the FBKTPlus algorithm.
   * This function performs the FBKT selection process and returns the best alternative.
   *
   * @param ALT List of SampleGenerate objects representing the alternatives.
   * @param params Array of parameters required by the FBKTPlus algorithm.
   * @return The best alternative (SampleGenerate object) after selection.
   */
  private def select(ALT: List[SampleGenerate], params: Array[Double]): SampleGenerate = {
    val selection = new FBKTWithinCore(ALT.asJava, params)
    selection.FBKTSelect() // Perform FBKT selection
    selection.getBestAlt // Return the best alternative
  }

  /**
   * Function to generate a sample using the given parameters.
   * This function creates a new SampleGenerate object using the provided parameters.
   *
   * @param params Array of parameters required to generate the sample.
   * @return A new instance of SampleGenerate.
   */
  private def sampleGeneration(params: Array[Double]): SampleGenerate = {
    new SampleGenerate(params) // Create a new SampleGenerate instance
  }

  /**
   * Function to generate samples from input parameters, with or without using a random seed.
   *
   * This function processes an RDD of strings, where each string contains an ID and a set of parameters.
   * It then generates samples using either a seeded random number generator or without seeding,
   * depending on the useSeed flag. The generated samples are paired with their corresponding IDs.
   *
   * @param sampleParam An RDD of strings, where each string represents an ID and a set of parameters, space-separated.
   * @param seed An integer seed used to initialize the random number generator for reproducibility.
   * @return An RDD of tuples, where each tuple contains an ID (Int) and a SampleGenerate object.
   */
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
   * Function to parse a line of input text into an ID and an array of parameters.
   *
   * This function takes a line of input, splits it into components, converts the first element
   * into an integer ID and the remaining elements into a double array of parameters.
   *
   * @param line A line of input text in the format "ID param1 param2 ... paramN".
   * @return A tuple containing:
   *         - ID: An integer representing the ID.
   *         - params: An array of doubles representing the parameters.
   */
  private def parseLine(line: String): (Int, Array[Double]) = {
    val param = line.split(" ") // Split the line by spaces to separate the ID and parameters
    val id = param(0).toInt // Convert the first element to an integer ID
    val params = param.map(_.toDouble) // Convert the entire array to doubles (ID will also be converted, but only the parameters are used)
    (id, params) // Return a tuple containing the ID and the parameters array
  }

  /**
   * Function to run the FBKT algorithm.
   * This function orchestrates the FBKT process by reading input data, generating samples,
   * partitioning them, and finally selecting the best alternative.
   *
   * @param sc SparkContext used for distributed computing.
   * @param paramOfSelect Array of parameters required by the FBKT process.
   * @param nCores Number of CPU cores to use.
   * @param inputPath Path to the input data file.
   * @param seed Seed for random number generation.
   * @param repeat Number of repetitions of the FBKT process.
   * @return A sequence of tuples containing:
   *         - Int: Replication number.
   *         - Int: ID of the best alternative selected.
   *         - Double: Wall-clock time in seconds.
   *         - Double: Total simulation time in seconds.
   */
  def runFBKT(sc: SparkContext, paramOfSelect: Array[Double], nCores: Int, inputPath: String, seed: Int, repeat: Int): Seq[(Int, Int, Double, Double)] = {
    val results = new scala.collection.mutable.ArrayBuffer[(Int, Int, Double, Double)]() // Buffer to store results
    var count = 0 // Initialize repetition counter
    val randomSeedGenerator = new Random(seed) // Initialize random seed

    // Repeat the FBKT algorithm for the specified number of times
    while (count < repeat) {
      val startTime = System.nanoTime() // Record start time
      val repeatSeed = randomSeedGenerator.nextInt() // Generate a common random number

      // Read input data and generate samples
      val sampleParam = sc.textFile(inputPath, nCores)
      val sample = generateSample(sampleParam, repeatSeed)

      // Partition the samples and cache them
      val uniformSampleKey = sample.partitionBy(new HashPartitioner(nCores))
      val uniformSample = uniformSampleKey.map(_._2).glom().map(_.toList)

      // Perform the FBKT selection
      val bestAltCore = uniformSample.map(ALT => select(ALT, paramOfSelect))

      // Reduce to find the best alternative
      val bestAlt = bestAltCore.reduce { (x, y) =>
        if (x.getMean > y.getMean) {
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
      val replication = count + 1 // Increment replication number
      val bestID = bestAlt.getId // Get ID of the best alternative
      val wallClockTime = (System.nanoTime() - startTime) / 1e9 // Calculate wall clock time in seconds
      val totalSimulateTime = bestAlt.getSimulateTime // Get total simulation time in seconds

      results += ((replication, bestID, wallClockTime, totalSimulateTime)) // Add results to buffer
      count += 1 // Increment counter
    }
    // Return the results as a sequence
    results
  }
}
