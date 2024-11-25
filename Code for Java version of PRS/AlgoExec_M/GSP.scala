package AlgoExec_M

import AlgoExec_W.GSPWithinCore
import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD
import SampleGen.SampleGenerate

import scala.collection.JavaConverters._
import scala.util.Random
import scala.util.control.Breaks._

object GSP {

  /**
   * Perform Stage 1 simulation on the provided alternatives.
   *
   * @param ALT List of SampleGenerate objects representing the alternatives.
   * @param params Array of parameters required for the simulation.
   * @return A tuple containing:
   *         - List of SampleGenerate objects (alternatives after simulation).
   *         - Double value representing the sum of simulation statistics (sumS).
   */
  private def stage1Simulation(ALT: List[SampleGenerate], params: Array[Double]): (List[SampleGenerate], Double) = {
    val stage1Sim = new GSPWithinCore(ALT.asJava, params)
    val sumS = stage1Sim.stage1Simulate()  // Perform stage 1 simulation
    (ALT, sumS)  // Return alternatives and sumS
  }

  /**
   * Perform Stage 1 screening on the provided alternatives.
   *
   * @param ALT List of SampleGenerate objects representing the alternatives.
   * @param params Array of parameters required for the screening.
   * @param sumS Double value representing the sum of simulation statistics from Stage 1.
   * @param eta Double value used as a screening threshold.
   * @return List of SampleGenerate objects that passed the screening.
   */
  private def stage1Screen(ALT: List[SampleGenerate], params: Array[Double], sumS: Double, eta: Double): List[SampleGenerate] = {
    val stage1Screen = new GSPWithinCore(ALT.asJava, params)
    stage1Screen.getBatchrMax(sumS)  // Set batch max based on sumS
    val alt = stage1Screen.screen(eta)  // Perform screening
    alt.asScala.toList  // Convert to Scala list and return
  }

  /**
   * Perform Stage 2 simulation on the provided alternatives.
   *
   * @param ALT List of SampleGenerate objects representing the alternatives.
   * @param params Array of parameters required for the simulation.
   * @return List of SampleGenerate objects after Stage 2 simulation.
   */
  private def stage2Simulation(ALT: List[SampleGenerate], params: Array[Double]): List[SampleGenerate] = {
    val stage2Sim = new GSPWithinCore(ALT.asJava, params)
    stage2Sim.simulate()  // Perform stage 2 simulation
    ALT  // Return alternatives
  }

  /**
   * Perform Stage 2 screening on the provided alternatives.
   *
   * @param ALT List of SampleGenerate objects representing the alternatives.
   * @param params Array of parameters required for the screening.
   * @param eta Double value used as a screening threshold.
   * @return List of SampleGenerate objects that passed the screening.
   */
  private def stage2Screen(ALT: List[SampleGenerate], params: Array[Double], eta: Double): List[SampleGenerate] = {
    val stage2Screen = new GSPWithinCore(ALT.asJava, params)
    val alt = stage2Screen.screen(eta)  // Perform screening
    alt.asScala.toList  // Convert to Scala list and return
  }

  /**
   * Perform Stage 3 simulation to determine the best alternative.
   *
   * @param ALT List of SampleGenerate objects representing the alternatives.
   * @param params Array of parameters required for the simulation.
   * @param h Double value used in the final selection process.
   * @return The best SampleGenerate object after Stage 3 simulation.
   */
  private def stage3Simulation(ALT: List[SampleGenerate], params: Array[Double], h: Double): SampleGenerate = {
    val stage3Simulate = new GSPWithinCore(ALT.asJava, params)
    val alt = stage3Simulate.stage3Simulate(h)  // Perform stage 3 simulation
    alt  // Return best alternative
  }

  /**
   * Generate a SampleGenerate object using the specified parameters.
   *
   * @param params Array of parameters for SampleGenerate.
   * @return A new instance of SampleGenerate.
   */
  private def sampleGeneration(params: Array[Double]): SampleGenerate = {
    new SampleGenerate(params)  // Create a new SampleGenerate instance
  }

  /**
   * Generate samples from input data, using or not using a random seed.
   *
   * @param sampleParam RDD containing lines of input data.
   * @param seed Int value used as the base seed for random number generation.
   * @return RDD of tuples containing:
   *         - Int value representing the ID of each sample.
   *         - SampleGenerate object representing the generated sample.
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
   * Parse a line of input text to extract an ID and an array of parameters.
   *
   * @param line A line of input text containing space-separated values.
   * @return A tuple containing:
   *         - Int value representing the ID extracted from the first element.
   *         - Array of Double values representing the parameters.
   */
  private def parseLine(line: String): (Int, Array[Double]) = {
    val param = line.split(" ") // Split line by spaces
    val id = param(0).toInt // The first element is the ID
    val params = param.map(_.toDouble) // Convert remaining elements to Double
    (id, params) // Return tuple of ID and parameters
  }

  /**
   * Function to run the GSP (Generalized Sequential Probability) algorithm.
   * This function performs the GSP process, which involves multiple stages of simulation and screening
   * to select the best alternative based on the provided parameters.
   *
   * @param sc SparkContext used for distributed computing.
   * @param paramOfSelect Array of parameters required for the GSP process.
   * @param nCores Number of CPU cores to use for parallel processing.
   * @param inputPath Path to the input data file containing parameters for each alternative.
   * @param seed Seed for random number generation.
   * @param eta Screening threshold value used in the GSP process.
   * @param rinottConstant Rinott constant used in the final stage of simulation.
   * @param repeat Number of repetitions of the GSP process.
   * @return A sequence of tuples containing:
   *         - Int: Replication number.
   *         - Int: ID of the best alternative selected.
   *         - Double: Wall-clock time in seconds.
   *         - Double: Total simulation time in seconds.
   */
  def runGSP(sc: SparkContext, paramOfSelect: Array[Double], nCores: Int, inputPath: String, seed: Int, eta: Double, rinottConstant: Double, repeat: Int): Seq[(Int, Int, Double, Double)] = {
    val results = new scala.collection.mutable.ArrayBuffer[(Int, Int, Double, Double)]() // Buffer to store results
    val randomSeedGenerator = new Random(seed) // Initialize random seed
    var count = 0  // Initialize repetition counter

    // Repeat the GSP algorithm for the specified number of repetitions
    while (count < repeat) {
      val startTime = System.nanoTime()  // Record start time
      val rMax = paramOfSelect(1).toInt  // Maximum number of batches
      val repeatSeed = randomSeedGenerator.nextInt() // Generate common random number

      // Read the parameters of each alternative and generate samples
      val sampleParam = sc.textFile(inputPath, nCores)
      val sample = generateSample(sampleParam, repeatSeed)

      // Partition the samples by key
      val uniformSampleKey = sample.partitionBy(new HashPartitioner(nCores))
      val uniformSample = uniformSampleKey.map(_._2).glom().map(_.toList)

      println("Enter the stage1!")
      // Perform stage 1 simulation
      val stage1Sim = uniformSample.map(l => stage1Simulation(l, paramOfSelect))
      val sumS = stage1Sim.map(_._2).reduce(_ + _)

      // Perform stage 1 screening
      val stage1Scr = stage1Sim.map(x => stage1Screen(x._1, paramOfSelect, sumS, eta))
      var screenResult = stage1Scr.flatMap(identity)
      var r = 0

      println("Enter the stage2!")
      breakable {
        // Perform stage 2 simulation and screening
        while (r < rMax) {
          val elements = screenResult.take(2)
          if (elements.length == 1) {
            val bestAlt = elements(0)
            // Collect results
            val replication = count + 1
            val bestID = bestAlt.getId // Get ID of best alternative
            val wallClockTime = (System.nanoTime() - startTime) / 1e9 // Calculate wall-clock time
            val totalSimulateTime = bestAlt.getSimulateTime // Get total simulation time

            results += ((replication, bestID, wallClockTime, totalSimulateTime)) // Add results to buffer
            break // Exit if only one alternative remains
          }

          // Sort and reassign IDs to screen results
          val screenResultSort = screenResult.map(x => (x.getId, x)).sortByKey()
          val screenResultReid = screenResultSort.zipWithIndex().map {
            case ((_, alt), idx) => (idx % nCores, alt)
          }

          // Partition and simulate in stage 2
          val uniformScreenResult = screenResultReid.partitionBy(new HashPartitioner(nCores))
          val uniformScreenResultList = uniformScreenResult.map(_._2).glom().map(_.toList)
          val stage2Sim = uniformScreenResultList.map(l => stage2Simulation(l, paramOfSelect))
          val stage2Scr = stage2Sim.map(l => stage2Screen(l, paramOfSelect, eta))
          r += 1

          if (r == rMax) {
            println("Enter the stage3!")
            // Perform stage 3 simulation
            val uniformStage3 = stage2Scr.flatMap(identity).repartition(nCores).glom().map(_.toList).filter(_.nonEmpty)
            val stage3ss = uniformStage3.map(x => stage3Simulation(x, paramOfSelect, rinottConstant))
            val bestAlt = stage3ss.reduce((x, y) => {
              if (x.getMean > y.getMean) { // Compare means
                x.setN(x.getN + y.getN) // Update sample size
                x.setSimulateTime(x.getSimulateTime + y.getSimulateTime) // Update simulation time
                x // Return x if its mean is higher
              } else {
                y.setN(y.getN + x.getN) // Update sample size
                y.setSimulateTime(y.getSimulateTime + x.getSimulateTime) // Update simulation time
                y // Return y if its mean is higher
              }
            })

            // Collect results
            val replication = count + 1
            val bestID = bestAlt.getId // Get ID of best alternative
            val wallClockTime = (System.nanoTime() - startTime) / 1e9 // Calculate wall-clock time
            val totalSimulateTime = bestAlt.getSimulateTime // Get total simulation time

            results += ((replication, bestID, wallClockTime, totalSimulateTime)) // Add results to buffer
          }
          screenResult = stage2Scr.flatMap(identity)
        }
      }
      count += 1
    }
    // Return the results as a sequence
    results
  }

  /**
   * Function to repartition an RDD by balancing the values across partitions.
   * This function collects the data, sorts it in descending order of the value, and then assigns each element
   * to the partition with the smallest total value sum to achieve a balanced distribution.
   *
   * @param rdd RDD containing pairs of SampleGenerate objects and their associated values (e.g., simulation times).
   * @param numPartitions The number of partitions to distribute the data into.
   * @return An RDD with the data repartitioned to balance the total values across partitions.
   */
  private def repartitionByBalancedValue(rdd: RDD[(SampleGenerate, Double)], numPartitions: Int): RDD[(SampleGenerate, Double)] = {
    // Step 1: Collect the data, ignoring empty partitions, and sort by the value in descending order
    val nonEmptyData = rdd.filter(_ => true).collect().filter(_ != null).sortBy(-_._2)

    // Step 2: Initialize partition sums and partition containers
    val partitionSums = Array.fill(numPartitions)(0.0)
    val partitions = Array.fill(numPartitions)(List.empty[(SampleGenerate, Double)])

    // Step 3: Greedily assign each element to the partition with the smallest total sum
    for ((key, value) <- nonEmptyData) {
      val minPartitionIndex = partitionSums.zipWithIndex.minBy(_._1)._2 // Find the partition with the smallest sum
      partitions(minPartitionIndex) = partitions(minPartitionIndex) :+ (key, value) // Add the element to the partition
      partitionSums(minPartitionIndex) += value // Update the sum for this partition
    }

    // Step 4: Convert partitions to a parallelized RDD and repartition
    val partitionedData = partitions.flatten // Flatten the list of lists
    val balancedRDD = rdd.context.parallelize(partitionedData, numPartitions) // Create a new RDD with balanced partitions

    balancedRDD
  }

  /**
   * Function to generate samples with or without using a seed for random number generation.
   * This function reads sample parameters, generates samples, and runs an initial stage 0 simulation to estimate
   * simulation time for each sample.
   *
   * @param sampleParam RDD containing lines of sample parameters as strings.
   * @param seed Seed for random number generation.
   * @param n0 Number of initial stage 0 simulations to run for each sample.
   * @return An RDD of pairs containing SampleGenerate objects and their estimated simulation times.
   */
  private def generateSampleWithStage0(sampleParam: org.apache.spark.rdd.RDD[String], seed: Int, n0: Int): org.apache.spark.rdd.RDD[(SampleGenerate, Double)] = {
    // Map over each line in the RDD and generate samples with a seeded random number generator
    sampleParam.map { line =>
      val (id, param) = parseLine(line) // Parse line into ID and parameters
      val alt = sampleGeneration(param) // Generate sample
      val estimateSimTime = alt.runSystemWithStage0(n0) // Run stage 0 simulation and estimate simulation time
      val random = new Random(seed + id) // Initialize random generator with seed based on ID
      val altSeed = random.nextInt() // Generate an alternative seed
      alt.setSeed(altSeed) // Set seed for the sample
      (alt, estimateSimTime) // Return the sample and estimated simulation time as a pair
    }
  }

  /**
   * Function to run the GSP algorithm with an initial Stage 0 simulation.
   * This function orchestrates the GSP process, including Stage 0 simulation, Stage 1 and Stage 2 simulations and screenings,
   * and finally Stage 3 simulation to select the best alternative.
   *
   * @param sc SparkContext used for distributed computing.
   * @param paramOfSelect Array of parameters required by the GSP process.
   * @param nCores Number of CPU cores to use.
   * @param inputPath Path to the input data file.
   * @param seed Seed for random number generation.
   * @param n0 Number of initial Stage 0 simulations to run for each sample.
   * @param eta Screening threshold value used in Stage 1 and Stage 2.
   * @param rinottConstant Rinott constant used in Stage 3 simulation.
   * @param repeat Number of repetitions of the GSP process.
   * @return A sequence of tuples containing the replication number, the best alternative ID, wall-clock time, and total simulation time.
   */
  def runGSPWithStage0(sc: SparkContext, paramOfSelect: Array[Double], nCores: Int, inputPath: String, seed: Int, n0: Int, eta: Double, rinottConstant: Double, repeat: Int): Seq[(Int, Int, Double, Double)] = {
    val results = new scala.collection.mutable.ArrayBuffer[(Int, Int, Double, Double)]() // Buffer to store results
    val randomSeedGenerator = new Random(seed) // Initialize random seed
    var count = 0  // Initialize repetition counter

    // Repeat the GSP algorithm for the specified number of repetitions
    while (count < repeat) {
      val startTime = System.nanoTime()  // Record start time
      val rMax = paramOfSelect(1).toInt  // Maximum number of batches
      val repeatSeed = randomSeedGenerator.nextInt() // Generate common random number

      // Read the parameters of each alternative and generate samples
      val sampleParam = sc.textFile(inputPath, nCores)
      println("Enter the stage0! ")
      val sample = generateSampleWithStage0(sampleParam, repeatSeed, n0)

      // Partition the samples
      val uniformSampleKey = repartitionByBalancedValue(sample, nCores)
      val uniformSample = uniformSampleKey.map(_._1).glom().map(_.toList)

      println("Enter the stage1! ")
      // Perform Stage 1 simulation
      val stage1Sim = uniformSample.map(l => stage1Simulation(l, paramOfSelect))
      val sumS = stage1Sim.map(_._2).reduce(_ + _)

      // Perform Stage 1 screening
      val stage1Scr = stage1Sim.map(x => stage1Screen(x._1, paramOfSelect, sumS, eta))
      var screenResult = stage1Scr.flatMap(identity)
      var r = 0

      println("Enter the stage2! ")
      breakable {
        // Perform Stage 2 simulation and screening
        while (r < rMax) {
          val elements = screenResult.take(2)
          if (elements.length == 1) {
            val bestAlt = elements(0)
            // Collect results
            val replication = count + 1
            val bestID = bestAlt.getId // Get ID of best alternative
            val wallClockTime = (System.nanoTime() - startTime) / 1e9 // Calculate wall clock time
            val totalSimulateTime = bestAlt.getSimulateTime // Get total simulation time

            results += ((replication, bestID, wallClockTime, totalSimulateTime)) // Add results to buffer
            break // Exit if only one alternative remains
          }

          // Repartition samples for Stage 2
          val screenResultMap = screenResult.map(x => (x, x.getEstimateSimTime))
          val uniformScreenResult = repartitionByBalancedValue(screenResultMap, nCores)
          val uniformScreenResultList = uniformScreenResult.map(_._1).glom().map(_.toList)

          // Perform Stage 2 simulation and screening
          val stage2Sim = uniformScreenResultList.map(l => stage2Simulation(l, paramOfSelect))
          val stage2Scr = stage2Sim.map(l => stage2Screen(l, paramOfSelect, eta))
          r += 1

          if (r == rMax) {
            println("Enter the stage3! ")
            // Perform Stage 3 simulation
            val stage2ScrMap = stage2Scr.flatMap(identity).map(x => (x, x.getEstimateSimTime))
            val uniformStage3 = repartitionByBalancedValue(stage2ScrMap, nCores)
            val uniformStage3List = uniformStage3.map(_._1).glom().map(_.toList)

            val stage3Sim = uniformStage3List.map(x => stage3Simulation(x, paramOfSelect, rinottConstant))
            val bestAlt = stage3Sim.reduce((x, y) => {
              if (x.getMean > y.getMean) { // Compare means
                x.setN(x.getN + y.getN) // Update sample size
                x.setSimulateTime(x.getSimulateTime + y.getSimulateTime) // Update simulation time
                x // Return x if its mean is higher
              } else {
                y.setN(y.getN + x.getN) // Update sample size
                y.setSimulateTime(y.getSimulateTime + x.getSimulateTime) // Update simulation time
                y // Return y if its mean is higher
              }
            })
            // Collect results
            val replication = count + 1
            val bestID = bestAlt.getId // Get ID of best alternative
            val wallClockTime = (System.nanoTime() - startTime) / 1e9 // Calculate wall clock time
            val totalSimulateTime = bestAlt.getSimulateTime // Get total simulation time

            results += ((replication, bestID, wallClockTime, totalSimulateTime)) // Add results to buffer
          }
          screenResult = stage2Scr.flatMap(identity)
        }
      }
      count += 1 // Increment counter
    }
    // Return the results as a sequence
    results
  }
}