package AlgoExec_W;

import SampleGen.SampleGenerate;

import java.util.ArrayList;
import java.util.List;

/**
 * Class representing the GSP (Generalized Selection Procedure) implementation within a core.
 * This class provides methods for simulating and screening alternatives to select the best one.
 */
public class GSPWithinCore {
    // List of all alternatives
    List<SampleGenerate> ALT;
    // Parameters of the procedure
    double[] param;

    // Getter for the list of alternatives
    public List<SampleGenerate> getALT() {
        return ALT;
    }

    // Setter for the list of alternatives
    public void setALT(List<SampleGenerate> ALT) {
        this.ALT = ALT;
    }

    // Getter for the parameters
    public double[] getParam() {
        return param;
    }

    // Setter for the parameters
    public void setParam(double[] param) {
        this.param = param;
    }

    /**
     * Constructor to initialize the list of alternatives and the parameters.
     *
     * @param ALT   List of SampleGenerate objects representing the alternatives.
     * @param param Array of parameters required by the procedure.
     */
    public GSPWithinCore(List<SampleGenerate> ALT, double[] param) {
        this.ALT = ALT;
        this.param = param;
    }

    /**
     * Perform Stage 1 simulation for all alternatives.
     * This method generates initial samples and calculates the variance for each alternative.
     *
     * @return The sum of the standard deviations of all alternatives.
     */
    public double stage1Simulate() {
        int n1 = (int) param[0]; // Initial number of samples
        double[] s2 = new double[ALT.size()]; // Array to store variances
        double sumS = 0; // Sum of standard deviations

        // Iterate through all alternatives
        for (int i = 0; i < ALT.size(); i++) {
            double[] sample = new double[n1]; // Array to store samples
            double mean = 0; // Mean of samples

            // Generate n1 samples for the current alternative
            for (int j = 0; j < n1; j++) {
                sample[j] = ALT.get(i).runSystem(); // Run the system to get a sample
                mean += sample[j] / n1; // Accumulate the mean
            }

            // Calculate the variance of the samples
            for (int j = 0; j < n1; j++) {
                s2[i] += Math.pow(sample[j] - mean, 2) / (n1 - 1); // Calculate and accumulate the variance
            }
            ALT.get(i).setS2(s2[i]); // Set the variance for the current alternative
            sumS += Math.sqrt(s2[i]); // Accumulate the standard deviation
        }
        return sumS; // Return the sum of standard deviations
    }

    /**
     * Set batch size and maximum number of samples for each alternative based on the average standard deviation.
     *
     * @param sumS The sum of standard deviations of all alternatives.
     */
    public void getBatchrMax(double sumS) {
        int n1 = (int) param[0]; // Initial number of samples
        int rMax = (int) param[1]; // Maximum number of batches
        int beta = (int) param[2]; // Scaling factor for the batch size
        int k = (int) param[5]; // Number of alternatives

        // Iterate through all alternatives
        for (SampleGenerate sampleGenerate : ALT) {
            int batch = (int) Math.ceil(beta * (Math.sqrt(sampleGenerate.getS2()) / (sumS / k))); // Calculate the batch size
            sampleGenerate.setBatch(batch); // Set the batch size
            sampleGenerate.setnMax(n1 + rMax * batch); // Set the maximum number of samples
        }
    }

    /**
     * Screen the alternatives and eliminate those that are unlikely to be the best.
     *
     * @param eta The screening threshold.
     * @return A list of alternatives that pass the screening.
     */
    public List<SampleGenerate> screen(double eta) {
        List<SampleGenerate> alt = new ArrayList<>(); // List to store the remaining alternatives
        int h = ALT.size();
        int countN = 0; // Count of samples for eliminated alternatives
        double simulateTime = 0.0; // Accumulated simulation time for eliminated alternatives
        int n1 = (int) param[0]; // Initial number of samples
        int[] elimination = new int[h]; // Array to track eliminations

        // Compare each pair of alternatives
        double[] Mean = new double[h];
        double[] C1 = new double[h];
        double[] C2 = new double[h];
        Mean[0] = ALT.get(0).getMean();
        C1[0] = ALT.get(0).getS2() / ALT.get(0).getNum();
        C2[0] = ALT.get(0).getS2() / ALT.get(0).getnMax();
        for (int i = 1; i < h; i++) {
            Mean[i] = ALT.get(i).getMean();
            C1[i] = ALT.get(i).getS2() / ALT.get(i).getNum();
            C2[i] = ALT.get(i).getS2() / ALT.get(i).getnMax();
            for (int j = 0; j < i; j++) {
                double y = (Mean[i] - Mean[j]) / (C1[i] + C1[j]); // Calculate the difference in means
                double a = eta * Math.sqrt((n1 - 1) / (C2[i] + C2[j])); // Calculate the threshold
                // Check if the alternatives should be eliminated
                if (y < -a) {
                    elimination[i] += 1;
                }
                if (-y < -a) {
                    elimination[j] += 1;
                }
            }
        }
        // Eliminate alternatives based on the number of eliminations
        for (int i = ALT.size() - 1; i >= 0; i--) {
            if (elimination[i] < 1) {
                alt.add(ALT.get(i)); // Keep the alternative
            } else {
                countN += (ALT.get(i).getN() + ALT.get(i).getNum()); // Accumulate the sample count
                simulateTime += ALT.get(i).getSimulateTime(); // Accumulate the simulation time
            }
        }
        // Update the remaining alternatives with the accumulated values
        alt.get(0).setN(countN + alt.get(0).getN());
        alt.get(0).setSimulateTime(alt.get(0).getSimulateTime() + simulateTime);
        return alt; // Return the list of remaining alternatives
    }

    /**
     * Simulate each alternative based on its batch size.
     */
    public void simulate() {
        // Iterate through all alternatives
        for (SampleGenerate sampleGenerate : ALT) {
            // Generate samples for each alternative based on the batch size
            for (int j = 0; j < sampleGenerate.getBatch(); j++) {
                sampleGenerate.runSystem(); // Run the system to get a sample
            }
        }
    }

    /**
     * Perform Stage 3 simulation to finalize the selection of the best alternative.
     *
     * @param h The Rinott constant used to calculate the number of additional samples.
     * @return The best alternative after the simulation.
     */
    public SampleGenerate stage3Simulate(double h) {
        int N = 0; // Total number of samples
        double[] mean = new double[ALT.size()]; // Array to store means of alternatives
        double simulationTime = 0.0; // Total simulation time

        // Iterate through all alternatives
        for (int i = 0; i < ALT.size(); i++) {
            double addN = Math.max(0, Math.ceil(h * h * ALT.get(i).getS2() / (param[6] * param[6])) - ALT.get(i).getNum()); // Calculate the number of additional samples
            // Generate additional samples for the current alternative
            for (int j = 0; j < addN; j++) {
                ALT.get(i).runSystem(); // Run the system to get a sample
            }
            N += (ALT.get(i).getN() + ALT.get(i).getNum()); // Accumulate the total number of samples
            simulationTime += ALT.get(i).getSimulateTime(); // Accumulate the total simulation time
            mean[i] = ALT.get(i).getMean(); // Store the mean of the current alternative
        }

        // Find the index of the alternative with the maximum mean
        int maxIndex = findMaxIndex(mean);
        ALT.get(maxIndex).setN(N); // Set the total number of samples for the best alternative
        ALT.get(maxIndex).setSimulateTime(simulationTime); // Set the total simulation time for the best alternative
        return ALT.get(maxIndex); // Return the best alternative
    }

    /**
     * Find the index of the maximum value in an array.
     *
     * @param array The array of values.
     * @return The index of the maximum value.
     * @throws IllegalArgumentException if the array is null or empty.
     */
    public static int findMaxIndex(double[] array) {
        if (array == null || array.length == 0) {
            throw new IllegalArgumentException("Array must not be null or empty"); // Validate the input array
        }
        int maxIndex = 0; // Initialize the index of the maximum value
        double maxValue = array[0]; // Initialize the maximum value

        // Iterate through the array to find the maximum value
        for (int i = 1; i < array.length; i++) {
            if (array[i] > maxValue) {
                maxValue = array[i]; // Update the maximum value
                maxIndex = i; // Update the index of the maximum value
            }
        }
        return maxIndex; // Return the index of the maximum value
    }
}

