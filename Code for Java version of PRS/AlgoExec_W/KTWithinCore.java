package AlgoExec_W;

import SampleGen.SampleGenerate;
import Utilities.Rinott;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Class representing the KT (Knapsack Test) procedure within a core.
 * This class provides methods for simulating and screening alternatives to select the best one using the KT algorithm.
 */
public class KTWithinCore {
    // The best alternative selected by the procedure
    SampleGenerate bestAlt;
    // List of all alternatives
    List<SampleGenerate> ALT;
    // Parameters of the procedure
    double[] param;

    // Getter for the best alternative
    public SampleGenerate getBestAlt() {
        return bestAlt;
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
     * @param param Array of parameters required by the KT procedure.
     */
    public KTWithinCore(List<SampleGenerate> ALT, double[] param) {
        this.ALT = ALT;
        this.param = param;
    }

    /**
     * Main method to execute the KT selection procedure.
     * This method performs the KT selection process to find the best alternative.
     */
    public void KTSelect() {
        // Retrieve parameters for the KT procedure
        double alpha = param[0]; // Significance level
        double delta = param[1]; // Indifference zone parameter
        int n0 = (int) param[2]; // Initial sample size
        int m = (int) param[3]; // Number of cores
        int g = (int) param[4]; // Number of alternatives in a group

        int nAlt = ALT.size(); // Number of alternatives
        int R = (int) Math.ceil(Math.log(nAlt) / Math.log(g)); // Number of rounds needed
        ArrayList<SampleGenerate> Ir = new ArrayList<>(ALT); // Copy the list of alternatives
        Ir.sort(Comparator.comparingInt(SampleGenerate::getId)); // Sort alternatives by ID for reproducibility

        // Perform the selection procedure over R rounds
        for (int i = 0; i < R; i++) {
            ArrayList<SampleGenerate> Ir1 = new ArrayList<>();
            int r = i + 1;
            double alphaOfr = alpha / (Math.pow(2, r)); // Calculate alpha for this round
            int numGroups = (int) Math.ceil((double) Ir.size() / g); // Number of groups

            // Partition the alternatives into groups and select the best from each group
            for (int j = 0; j < numGroups; j++) {
                List<SampleGenerate> group = new ArrayList<>();
                for (int k = 0; k < Ir.size(); k++) {
                    if (k % numGroups == j) {
                        group.add(Ir.get(k));
                    }
                }
                SampleGenerate survivorAlt = KN(group, n0, alphaOfr, delta); // Select the best alternative from the group
                Ir1.add(survivorAlt); // Add to the list for the next round
            }
            Ir = Ir1; // Update the list of alternatives
        }

        // Start the final selection stage using Rinott's procedure
        int rm = R + 1;
        double alphaMax = (alpha / (Math.pow(2, rm)));
        double h = Rinott.rinott(m, 1 - alphaMax, n0 - 1); // Calculate the Rinott constant
        double mean = 0;
        double si2 = 0;

        // Collect initial samples and calculate variance
        for (int i = 0; i < n0; i++) {
            double sample = Ir.get(0).runSystem();
            si2 += sample * sample;
            mean += sample / n0;
        }

        double s2 = (si2 - n0 * mean * mean) / (n0 - 1);
        double sumSample = mean * n0;
        double constant = Math.ceil(Math.pow(h / delta, 2) * s2);

        // Collect additional samples if necessary
        for (int i = 0; i < Math.max(0, constant - n0); i++) {
            double addSample = Ir.get(0).runSystem();
            sumSample += addSample;
        }

        mean = sumSample / Math.max(n0, constant);
        Ir.get(0).setMean(mean); // Set the mean for the best alternative
        Ir.get(0).setN(Ir.get(0).getN() + Ir.get(0).getNum());
        bestAlt = Ir.get(0); // Set the best alternative
    }

    /**
     * The KN (Known-Number) procedure to select the best alternative from a group.
     *
     * @param ALTGroup List of SampleGenerate objects representing the alternatives in the group.
     * @param n0       Initial sample size.
     * @param alpha    Significance level for the group.
     * @param delta    Indifference zone parameter for the group.
     * @return The best alternative from the group.
     */
    private SampleGenerate KN(List<SampleGenerate> ALTGroup, int n0, double alpha, double delta) {
        int nAlt = ALTGroup.size();
        double h2 = (n0 - 1) * (Math.pow(2.0 * alpha / (nAlt - 1), -2.0 / (n0 - 1)) - 1);
        List<Integer> index = new ArrayList<>();
        double[] sumSample = new double[nAlt];
        double[][] sample = new double[nAlt][n0];

        // Collect initial samples for each alternative
        for (int i = 0; i < nAlt; i++) {
            for (int j = 0; j < n0; j++) {
                sample[i][j] = ALTGroup.get(i).runSystem();
                sumSample[i] += sample[i][j];
            }
            index.add(i);
        }

        // Calculate pairwise variance estimates
        double[][] s2 = new double[nAlt][nAlt];
        for (int i = 1; i < nAlt; i++) {
            for (int j = 0; j < i; j++) {
                for (int k = 0; k < n0; k++) {
                    s2[i][j] += Math.pow((sample[i][k] - sample[j][k]) - (sumSample[i] - sumSample[j]) / n0, 2.0) / (n0 - 1);
                }
                s2[j][i] = s2[i][j];
            }
        }

        int t = n0; // Current sample size
        int nSimulation = 0; // Total number of simulations
        double simulateTime = 0.0; // Total simulation time

        // Continue sampling until only one alternative remains
        while (index.size() > 1) {
            int[] elimination = new int[index.size()];

            // Compare pairs of alternatives and eliminate one
            for (int i = 1; i < index.size(); i++) {
                for (int j = 0; j < i; j++) {
                    double N = Math.floor(s2[index.get(i)][index.get(j)] * h2 / (delta * delta));
                    if (t > N) {
                        if (sumSample[index.get(i)] > sumSample[index.get(j)]) {
                            elimination[j] += 1;
                        } else {
                            elimination[i] += 1;
                        }
                    } else {
                        if (sumSample[index.get(i)] - sumSample[index.get(j)] < -Math.max(h2 * s2[index.get(i)][index.get(j)] / (2 * delta) - delta * t / 2.0, 0)) {
                            elimination[i] += 1;
                        } else if (sumSample[index.get(j)] - sumSample[index.get(i)] < -Math.max(h2 * s2[index.get(i)][index.get(j)] / (2 * delta) - delta * t / 2.0, 0)) {
                            elimination[j] += 1;
                        }
                    }
                }
            }

            // Remove eliminated alternatives
            for (int i = elimination.length - 1; i >= 0; i--) {
                if (elimination[i] != 0) {
                    simulateTime += ALTGroup.get(index.get(i)).getSimulateTime();
                    nSimulation += (ALTGroup.get(index.get(i)).getN() + ALTGroup.get(index.get(i)).getNum());
                    index.remove(i);
                }
            }

            // Break if only one alternative remains
            if (index.size() <= 1) {
                break;
            }

            // Collect additional samples for remaining alternatives
            for (Integer integer : index) {
                sumSample[integer] += ALTGroup.get(integer).runSystem();
            }
            t++;
        }
        // Update the best alternative's sample count and simulation time
        ALTGroup.get(index.get(0)).setN(ALTGroup.get(index.get(0)).getN() + nSimulation);
        ALTGroup.get(index.get(0)).setSimulateTime(ALTGroup.get(index.get(0)).getSimulateTime() + simulateTime);
        return ALTGroup.get(index.get(0)); // Return the best alternative
    }
}