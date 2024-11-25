package UserInterface

import AlgoExec_M.FBKT.runFBKT
import AlgoExec_M.GSP.{runGSP, runGSPWithStage0}
import AlgoExec_M.KT.runKT
import org.apache.spark.{SparkConf, SparkContext}
import Utilities.{EtaFunc, Rinott}

import java.io.PrintWriter
import java.nio.file.{Paths, Files}
import scala.collection.mutable.ListBuffer
import scala.swing._
import scala.swing.event._

/**
 * GUI object for selecting and running different simulation procedures (GSP, KT, FBKT).
 * Extends SimpleSwingApplication to create a graphical user interface using Scala Swing.
 */
object GUI extends SimpleSwingApplication {
  // Paths to the input parameters and output file
  private var parametersPath: String = ""
  private var outputFilePath: String = ""

  /**
   * Main method to initialize the application and set the parameters' path.
   * Expects the path to the "alternatives.txt" file as the first command-line argument.
   *
   * @param args Command line arguments, where the first argument should be the path to alternatives.txt.
   */
  override def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("Error: You must provide the path to alternatives.txt as the first argument.")
      System.exit(1)
    } else {
      parametersPath = args(0)
      val directory = Paths.get(parametersPath).getParent.toString
      outputFilePath = Paths.get(directory, "output.txt").toString
    }
    super.main(args)
  }

  /**
   * Main frame of the GUI application.
   */
  def top: Frame = new MainFrame {
    title = "Procedure Selector"
    preferredSize = new Dimension(635, 700)
    centerOnScreen()

    // Dimensions for text fields and labels
    val fieldWidth = 250
    val fieldHeight = 25

    // Dropdown menu for selecting an algorithm
    val algorithmChoice: ComboBox[String] = new ComboBox(Seq("GSP", "KT", "FBKT")) {
      preferredSize = new Dimension(fieldWidth, fieldHeight)
      maximumSize = new Dimension(fieldWidth, fieldHeight)
    }
    algorithmChoice.selection.index = -1 // Initialize selection to be unselected

    // Panel for displaying input fields for parameters
    val paramPanel: GridPanel = new GridPanel(12, 2) {
      hGap = 13
      vGap = 5
      preferredSize = new Dimension(480, 400)
      maximumSize = new Dimension(480, 400)
    }
    val inputFields: ListBuffer[TextField] = ListBuffer[TextField]()

    // Main layout of the GUI
    contents = new BoxPanel(Orientation.Vertical) {
      contents += Swing.VStrut(10)
      contents += new BoxPanel(Orientation.Horizontal) {
        contents += Swing.HGlue
        contents += new BoxPanel(Orientation.Vertical) {
          contents += new Label("Select Procedure:") {
            horizontalAlignment = Alignment.Center
          }
          contents += Swing.VStrut(7)
          contents += algorithmChoice
        }
        contents += Swing.HGlue
      }
      contents += Swing.VStrut(10)
      contents += paramPanel
      contents += Swing.VGlue
      border = Swing.EmptyBorder(10, 10, 10, 10)
    }

    // Listener for algorithm selection changes
    listenTo(algorithmChoice.selection)
    reactions += {
      case SelectionChanged(`algorithmChoice`) =>
        updateParameterFields(algorithmChoice.selection.item)
    }

    /**
     * Updates the parameter input fields based on the selected algorithm.
     *
     * @param algorithm The selected algorithm name.
     */
    def updateParameterFields(algorithm: String): Unit = {
      paramPanel.contents.clear() // Clear existing parameter fields
      inputFields.clear() // Clear existing input fields

      if (algorithm.nonEmpty) {
        // Define parameter labels and tooltips based on the selected algorithm
        val labelsAndTooltips = algorithm match {
          case "GSP" => Seq(
            ("n0 (default=0)", "Sample size of Stage 0."),
            ("n1", "Sample size of Stage 1."),
            ("rMax", "The maximum number of rounds in Stage 2."),
            ("beta", "Average number of simulation samples taken from an alternative in each round in Stage 2."),
            ("alpha1", "The splits of the tolerable probability of incorrect selection alpha such that alpha = alpha1 + alpha2."),
            ("alpha2", "The splits of the tolerable probability of incorrect selection alpha such that alpha = alpha1 + alpha2."),
            ("delta", "IZ parameter."),
            ("# of processors", "Number of processors used to run the procedure."),
            ("# of alternatives", "Number of alternatives for the problem."),
            ("Seed (default=1234)", "Seed used to generate random numbers."),
            ("Repeat (default=1)", "The number of times the problem is repeatedly solved.")
          )
          case "KT" => Seq(
            ("alpha", "Tolerable probability of incorrect selection."),
            ("delta", "IZ parameter."),
            ("n0", "First-stage sample size when using the KN procedure."),
            ("g", "Number of alternatives in a group."),
            ("# of processors", "Number of processors used to run the procedure."),
            ("# of alternatives", "Number of alternatives for the problem."),
            ("Seed (default=1234)", "Seed used to generate random numbers."),
            ("Repeat (default=1)", "The number of times the problem is repeatedly solved.")
          )
          case "FBKT" => Seq(
            ("N", "Total sample budget."),
            ("n0", "Sample size needed at the initial stage for seeding."),
            ("phi", "A positive integer that determines budget allocation."),
            ("# of processors", "Number of processors used to run the procedure."),
            ("# of alternatives", "Number of alternatives for the problem."),
            ("Seed (default=1234)", "Seed used to generate random numbers."),
            ("Repeat (default=1)", "The number of times the problem is repeatedly solved.")
          )
        }

        // Create and add input fields for each parameter
        labelsAndTooltips.foreach { case (label, tooltip) =>
          val field = new TextField {
            columns = 14
            preferredSize = new Dimension(fieldWidth, fieldHeight)
            maximumSize = new Dimension(fieldWidth, fieldHeight)
          }
          val labelComponent = new Label(label) {
            preferredSize = new Dimension(fieldWidth, fieldHeight)
            maximumSize = new Dimension(fieldWidth, fieldHeight)
          }
          labelComponent.tooltip = tooltip
          paramPanel.contents += labelComponent
          paramPanel.contents += field
          inputFields += field
        }

        // Run button to execute the selected algorithm
        val runButton = new Button("Run")

        // Listener for the run button
        listenTo(runButton)
        reactions += {
          case ButtonClicked(`runButton`) =>
            val params = inputFields.map(_.text.trim).toArray
            // Run the selected algorithm with the provided parameters
            algorithmChoice.selection.item match {
              case "GSP" => runAlgorithm(params, validateGSPArgs, runGSPWrapper)
              case "KT" => runAlgorithm(params, validateKTArgs, runKTWrapper)
              case "FBKT" => runAlgorithm(params, validateFBKTArgs, runFBKTWrapper)
            }
        }

        paramPanel.contents += Swing.VStrut(10)
        paramPanel.contents += new BoxPanel(Orientation.Horizontal) {
          contents += Swing.HGlue
          contents += runButton
          contents += Swing.HGlue
        }

        paramPanel.revalidate()
        paramPanel.repaint() // Refresh the parameter panel
      }
    }

    /**
     * Appends formatted output to the specified output file.
     *
     * @param algorithm The name of the algorithm used (e.g., "KT", "GSP", "FBKT").
     * @param params An array of parameter values used in the algorithm.
     * @param results A sequence of tuples containing replication, the best alternative ID, wall-clock time, and total simulation time.
     */
    def appendFormattedOutput(algorithm: String, params: Array[String], results: Seq[(Int, Int, Double, Double)]): Unit = {
      // Create a writer to write output to the specified file, using UTF-8 encoding and overwriting the file if it exists
      val writer = new PrintWriter(Files.newBufferedWriter(
        Paths.get(outputFilePath),
        java.nio.charset.StandardCharsets.UTF_8,
        java.nio.file.StandardOpenOption.CREATE,
        java.nio.file.StandardOpenOption.TRUNCATE_EXISTING
      ))

      try {
        // Write the chosen procedure and its input parameters
        writer.println(s"Chosen Procedure: The $algorithm Procedure")
        writer.println()
        writer.println("Input Parameters:")

        // Format the parameters based on the selected algorithm
        algorithm match {
          case "KT" =>
            writer.println(s"alpha = ${params(0)}, delta = ${params(1)}, n0 = ${params(2)}, g = ${params(3)}, # of processors = ${params(4)}, # of alternatives = ${params(5)}, seed = ${params(6)}, and repeat = ${params(7)}.")
          case "GSP" =>
            writer.println(s"n0 = ${params(0)}, n1 = ${params(1)}, rMax = ${params(2)}, beta = ${params(3)}, alpha1 = ${params(4)}, alpha2 = ${params(5)}, delta = ${params(6)}, # of processors = ${params(7)}, # of alternatives = ${params(8)}, seed = ${params(9)}, and repeat = ${params(10)}.")
          case "FBKT" =>
            writer.println(s"N = ${params(0)}, n0 = ${params(1)}, phi = ${params(2)}, # of processors = ${params(3)}, # of alternatives = ${params(4)}, seed = ${params(5)}, and repeat = ${params(6)}.")
        }

        writer.println()
        // Write the headers for the results table
        writer.println("BestID: The index of the selected best alternative.")
        writer.println("WCT: Wall-Clock Time.")
        writer.println("TST: Total Simulation Time.")
        writer.println()

        // Define the format for the table headers and rows
        val headerFormat = "%-15s%-12s%-18s%-18s"
        val rowFormat = "%-15d%-12d%-18s%-18s"

        // Print the header for the results table
        writer.println(String.format(headerFormat, "Replication", "BestID", "WCT (Seconds)", "TST (Seconds)"))

        // Write each result in the specified format
        results.foreach { case (replication, bestID, wallClockTime, totalSimulateTime) =>
          writer.println(String.format(rowFormat,
            Int.box(replication), // Box the primitive int to use in String.format
            Int.box(bestID), // Box the primitive int to use in String.format
            formatTime(wallClockTime), // Format wall-clock time
            formatTime(totalSimulateTime) // Format total simulation time
          ))
        }
      } finally {
        writer.close() // Ensure the writer is closed to release resources
      }
    }

    /**
     * Formats time values for output.
     *
     * @param time The time value to format.
     * @return A formatted string representation of the time. If time is >= 1, format with two decimal places.
     *         Otherwise, format in scientific notation with two decimal places.
     */
    def formatTime(time: Double): String = {
      if (time >= 1) f"$time%.2f" // Format with two decimal places if time is greater than or equal to 1
      else f"$time%.2e" // Format in scientific notation if time is less than 1
    }

    /**
     * Runs the GSP (Generalized Selection Procedure) algorithm with the provided parameters.
     *
     * @param params Array of parameters for the GSP algorithm:
     *               - params(0): n0 (Sample size of Stage 0, default 0 if empty)
     *               - params(1): n1 (Sample size of Stage 1)
     *               - params(2): rMax (Maximum number of rounds in Stage 2)
     *               - params(3): beta (Average number of simulation samples taken from an alternative in each round)
     *               - params(4): alpha1 (First split of the tolerable probability of incorrect selection)
     *               - params(5): alpha2 (Second split of the tolerable probability of incorrect selection)
     *               - params(6): delta (IZ parameter)
     *               - params(7): nProcessors (Number of processors to use)
     *               - params(8): nAlternatives (Number of alternatives)
     *               - params(9): seed (Random seed, default 1234 if empty)
     *               - params(10): repeat (Number of repetitions, default 1 if empty)
     */
    def runGSPWrapper(params: Array[String]): Unit = {
      try {
        // Parse parameters with defaults for empty values
        val n0 = if (params(0).isEmpty) 0 else params(0).toInt
        val n1 = params(1).toInt
        val rMax = params(2).toInt
        val beta = params(3).toInt
        val alpha1 = params(4).toDouble
        val alpha2 = params(5).toDouble
        val delta = params(6).toDouble
        val nProcessors = params(7).toInt
        val nAlternatives = params(8).toInt
        val seed = if (params(9).isEmpty) 1234 else params(9).toInt
        val repeat = if (params(10).isEmpty) 1 else params(10).toInt

        // Configure Spark and initialize context
        val sparkConf = new SparkConf().setAppName("GSP").set("spark.cores.max", nProcessors.toString)
        val sc = new SparkContext(sparkConf)
        val paramOfSelect = Array(n1, rMax, beta, alpha1, alpha2, nAlternatives, delta)
        val eta = EtaFunc.find_eta(n1, alpha1, nAlternatives)
        val rinottConstant = Rinott.rinott(nAlternatives, 1 - alpha2, n1 - 1)

        // Run GSP with or without Stage 0
        val results = if (n0 == 0) {
          runGSP(sc, paramOfSelect, nProcessors, parametersPath, seed, eta, rinottConstant, repeat)
        } else {
          runGSPWithStage0(sc, paramOfSelect, nProcessors, parametersPath, seed, n0, eta, rinottConstant, repeat)
        }
        val inputParams = Array(n0.toString, n1.toString, rMax.toString, beta.toString, alpha1.toString, alpha2.toString, delta.toString, nProcessors.toString, nAlternatives.toString, seed.toString, repeat.toString)

        sc.stop() // Stop SparkContext
        appendFormattedOutput("GSP", inputParams, results) // Output formatted results

      } catch {
        case ex: Exception =>
          ex.printStackTrace()
          println(s"Error occurred while running GSP: ${ex.getMessage}")
      }
    }

    /**
     * Runs the KT (KN Procedure) algorithm with the provided parameters.
     *
     * @param params Array of parameters for the KT algorithm:
     *               - params(0): alpha (Tolerable probability of incorrect selection)
     *               - params(1): delta (IZ parameter)
     *               - params(2): n0 (Initial sample size)
     *               - params(3): g (Number of alternatives in a group)
     *               - params(4): nProcessors (Number of processors to use)
     *               - params(5): nAlternatives (Number of alternatives)
     *               - params(6): seed (Random seed, default 1234 if empty)
     *               - params(7): repeat (Number of repetitions, default 1 if empty)
     */
    def runKTWrapper(params: Array[String]): Unit = {
      try {
        // Parse parameters with defaults for empty values
        val alpha = params(0).toDouble
        val delta = params(1).toDouble
        val n0 = params(2).toInt
        val g = params(3).toInt
        val nProcessors = params(4).toInt
        val nAlternatives = params(5).toInt
        val seed = if (params(6).isEmpty) 1234 else params(6).toInt
        val repeat = if (params(7).isEmpty) 1 else params(7).toInt

        val inputParams = Array(alpha.toString, delta.toString, n0.toString, g.toString, nProcessors.toString, nAlternatives.toString, seed.toString, repeat.toString)
        val sparkConf = new SparkConf().setAppName("KT").set("spark.cores.max", nProcessors.toString)
        val sc = new SparkContext(sparkConf)
        val paramOfSelect = Array(alpha, delta, n0, nProcessors, g)

        val results = runKT(sc, paramOfSelect, nProcessors, parametersPath, seed, repeat)

        sc.stop() // Stop SparkContext
        appendFormattedOutput("KT", inputParams, results) // Output formatted results

      } catch {
        case ex: Exception =>
          ex.printStackTrace()
          println(s"Error occurred while running KT: ${ex.getMessage}")
      }
    }

    /**
     * Runs the FBKT (Fixed Budget KT Procedure) algorithm with the provided parameters.
     *
     * @param params Array of parameters for the FBKT algorithm:
     *               - params(0): N (Total sample budget)
     *               - params(1): n0 (Sample size needed at the initial stage, default 0 if empty)
     *               - params(2): phi (Budget allocation parameter)
     *               - params(3): nProcessors (Number of processors to use)
     *               - params(4): nAlternatives (Number of alternatives)
     *               - params(5): seed (Random seed, default 1234 if empty)
     *               - params(6): repeat (Number of repetitions, default 1 if empty)
     */
    def runFBKTWrapper(params: Array[String]): Unit = {
      try {
        // Parse parameters with defaults for empty values
        val N = params(0).toInt
        val n0 = if (params(1).isEmpty) 0 else params(1).toInt
        val phi = params(2).toInt
        val nProcessors = params(3).toInt
        val nAlternatives = params(4).toInt
        val seed = if (params(5).isEmpty) 1234 else params(5).toInt
        val repeat = if (params(6).isEmpty) 1 else params(6).toInt

        val inputParams = Array(N.toString, n0.toString, phi.toString, nProcessors.toString, nAlternatives.toString, seed.toString, repeat.toString)
        val sparkConf = new SparkConf().setAppName("FBKT").set("spark.cores.max", nProcessors.toString)
        val sc = new SparkContext(sparkConf)
        val N1 = Math.floor((N - n0 * nAlternatives)*1.0 / nProcessors)
        val paramOfSelect = Array(N1, n0, phi.toDouble)

        val results = runFBKT(sc, paramOfSelect, nProcessors, parametersPath, seed, repeat)
        sc.stop() // Stop SparkContext
        appendFormattedOutput("FBKT", inputParams, results) // Output formatted results

      } catch {
        case ex: Exception =>
          ex.printStackTrace()
          println(s"Error occurred while running FBKT: ${ex.getMessage}")
      }
    }

    /**
     * Runs an algorithm with the given parameters, after validating them.
     *
     * @param params Array of parameters as strings.
     * @param validateArgs Function to validate the parameters.
     * @param runFunc Function to execute the algorithm if parameters are valid.
     */
    def runAlgorithm(params: Array[String], validateArgs: Array[String] => Boolean, runFunc: Array[String] => Unit): Unit = {
      // Validate the parameters using the provided validation function
      if (!validateArgs(params)) {
        // Show an error dialog if validation fails
        Dialog.showMessage(contents.head, "Please provide correct parameter format.", title = "Error", Dialog.Message.Error)
        return
      }
      // Run the algorithm if validation passes
      runFunc(params)
    }

    /**
     * Validates parameters for the GSP (Generalized Selection Procedure) algorithm.
     *
     * @param args Array of parameters as strings. Expected:
     *             - args(1): n1 (must be > 0)
     *             - args(2): rMax (must be > 0)
     *             - args(3): beta (must be > 0)
     *             - args(4): alpha1 (must be between 0 and 1, exclusive)
     *             - args(5): alpha2 (must be between 0 and 1, exclusive)
     *             - args(6): delta (must be > 0)
     *             - args(7): Number of processors (must be > 0)
     *             - args(8): Number of alternatives (must be > 0)
     * @return True if parameters are valid, false otherwise.
     */
    def validateGSPArgs(args: Array[String]): Boolean = {
      try {
        if (args(1).toInt <= 0) return false
        if (args(2).toInt <= 0) return false
        if (args(3).toInt <= 0) return false
        if (args(4).toDouble <= 0 || args(4).toDouble >= 1) return false
        if (args(5).toDouble <= 0 || args(5).toDouble >= 1) return false
        if (args(6).toDouble <= 0) return false
        if (args(7).toInt <= 0) return false
        if (args(8).toInt <= 0) return false
        true
      } catch {
        case _: NumberFormatException => false // Return false if a number format exception occurs
      }
    }

    /**
     * Validates parameters for the KT (KN Procedure) algorithm.
     *
     * @param args Array of parameters as strings. Expected:
     *             - args(0): alpha (must be between 0 and 1, exclusive)
     *             - args(1): delta (must be > 0)
     *             - args(2): n0 (must be > 0)
     *             - args(3): g (must be > 0)
     *             - args(4): Number of processors (must be > 0)
     *             - args(5): Number of alternatives (must be > 0)
     * @return True if parameters are valid, false otherwise.
     */
    def validateKTArgs(args: Array[String]): Boolean = {
      try {
        if (args(0).toDouble <= 0 || args(0).toDouble >= 1) return false
        if (args(1).toDouble <= 0) return false
        if (args(2).toInt <= 0) return false
        if (args(3).toInt <= 0) return false
        if (args(4).toInt <= 0) return false
        if (args(5).toInt <= 0) return false
        true
      } catch {
        case _: NumberFormatException => false // Return false if a number format exception occurs
      }
    }

    /**
     * Validates parameters for the FBKT (Fixed Budget KT Procedure) algorithm.
     *
     * @param args Array of parameters as strings. Expected:
     *             - args(0): N (must be > 0)
     *             - args(1): n0 (must be > 0)
     *             - args(2): phi (must be > 0)
     *             - args(3): Number of processors (must be > 0)
     *             - args(4): Number of alternatives (must be > 0)
     * @return True if parameters are valid, false otherwise.
     */
    def validateFBKTArgs(args: Array[String]): Boolean = {
      try {
        if (args(0).toInt <= 0) return false
        if (args(1).toInt <= 0) return false
        if (args(2).toInt <= 0) return false
        if (args(3).toInt <= 0) return false
        if (args(4).toInt <= 0) return false
        true
      } catch {
        case _: NumberFormatException => false // Return false if a number format exception occurs
      }
    }

  }
}

