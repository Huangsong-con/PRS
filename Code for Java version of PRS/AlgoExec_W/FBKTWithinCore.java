package AlgoExec_W;

import SampleGen.SampleGenerate;

import java.util.ArrayList;
import java.util.List;

/**
 * The FBKTWithinCore class implements the FBKT selection process within a core.
 * It performs a sequential selection of alternatives based on mean performance
 * to identify the best alternative among a set of samples.
 */
public class FBKTWithinCore {

    // The best alternative selected by the procedure
    SampleGenerate bestAlt;

    // List of all alternatives to be compared
    List<SampleGenerate> ALT;

    // Parameters for the FBKT selection procedure
    double[] param;

    /**
     * Getter for the best alternative selected by the procedure.
     *
     * @return The best alternative (SampleGenerate object).
     */
    public SampleGenerate getBestAlt() {
        return bestAlt;
    }

    /**
     * Setter for the best alternative.
     *
     * @param bestAlt The best alternative to be set.
     */
    public void setBestAlt(SampleGenerate bestAlt) {
        this.bestAlt = bestAlt;
    }

    /**
     * Getter for the list of alternatives.
     *
     * @return The list of SampleGenerate objects representing the alternatives.
     */
    public List<SampleGenerate> getALT() {
        return ALT;
    }

    /**
     * Setter for the list of alternatives.
     *
     * @param ALT The list of alternatives to be set.
     */
    public void setALT(List<SampleGenerate> ALT) {
        this.ALT = ALT;
    }

    /**
     * Getter for the parameters of the FBKT procedure.
     *
     * @return Array of parameters for the FBKT procedure.
     */
    public double[] getParam() {
        return param;
    }

    /**
     * Setter for the parameters of the FBKT procedure.
     *
     * @param param Array of parameters to be set for the FBKT procedure.
     */
    public void setParam(double[] param) {
        this.param = param;
    }

    /**
     * Constructor to initialize the FBKTWithinCore object with a list of alternatives and parameters.
     *
     * @param ALT List of SampleGenerate objects representing the alternatives.
     * @param param Array of parameters required for the FBKT selection process.
     */
    public FBKTWithinCore(List<SampleGenerate> ALT, double[] param) {
        this.ALT = ALT;
        this.param = param;
    }

    /**
     * Main method to execute the FBKT selection procedure.
     * This method iteratively selects the best alternative based on mean performance
     * over multiple rounds, ultimately identifying a single best alternative.
     */
    public void FBKTSelect() {
        // Parameters for the FBKT selection procedure
        int N = (int) param[0]; // Total sample budget for each round
        int n0 = (int) param[1]; // Initial sample size for estimating mean
        int phi = (int) param[2]; // Allocation rule parameter
        int nAlt = ALT.size(); // Number of alternatives

        // Initial sampling for each alternative
        for (SampleGenerate sampleGenerate : ALT) {
            for (int j = 0; j < n0; j++) {
                sampleGenerate.runSystem(); // Generate initial samples
            }
        }

        // Maximum number of rounds in the FBKT procedure
        int R = (int) Math.ceil(Math.log(nAlt) / Math.log(2));
        List<SampleGenerate> Ir = ALT; // Current list of alternatives

        // Perform the selection procedure over R rounds
        for (int i = 0; i < R; i++) {
            List<SampleGenerate> Ir1 = new ArrayList<>(); // List for round survivors
            double[] mean = new double[Ir.size()]; // Array to store the mean of each alternative

            // Calculate mean for each alternative in the current round
            for (int j = 0; j < Ir.size(); j++) {
                mean[j] = Ir.get(j).getMean();
            }

            // Sort indices based on mean values
            int[] index = quickSort(mean);
            int r = i + 1; // Current round number
            // Calculate total number of samples for this round
            int Nr = (int) Math.floor((double) r / ((double) (phi - 1) * phi) * Math.pow((double) (phi - 1) / phi, r) * N);
            int Nl = (int) Math.floor(Nr / (double) Ir.size()); // Samples per alternative

            // Perform pairwise comparison for even or odd number of alternatives
            if (Ir.size() % 2 == 0) {
                // Even number of alternatives
                for (int j = 0; j < Ir.size() / 2; j++) {
                    SampleGenerate survivor = SelectOFTwo(Ir.get(index[j]), Ir.get(index[Ir.size() - 1 - j]), Nl);
                    Ir1.add(survivor); // Add survivor to next round list
                }
            } else {
                // Odd number of alternatives
                Ir1.add(Ir.get(index[Ir.size() - 1])); // Directly add alternative with the largest mean
                for (int j = 0; j < (Ir.size() - 1) / 2; j++) {
                    SampleGenerate survivor = SelectOFTwo(Ir.get(index[j]), Ir.get(index[Ir.size() - 2 - j]), Nl);
                    Ir1.add(survivor);
                }
            }
            Ir = Ir1; // Update alternatives for the next round
        }

        // Final simulation for the best alternative in the last round
        int Nr1 = (int) Math.floor((double) (R + 1) / ((double) (phi - 1) * phi) * Math.pow((double) (phi - 1) / phi, (R + 1)) * N);
        double meanNr1 = 0;
        for (int i = 0; i < Nr1; i++) {
            meanNr1 += Ir.get(0).runSystem() / Nr1; // Generate additional samples
        }
        Ir.get(0).setMean(meanNr1);
        Ir.get(0).setN(Ir.get(0).getNum() + Ir.get(0).getN());
        bestAlt = Ir.get(0); // Set the best alternative
    }

    /**
     * Select the better alternative between two alternatives using equal allocation.
     *
     * @param A First alternative.
     * @param B Second alternative.
     * @param n Number of samples to generate for each alternative.
     * @return The better alternative based on accumulated sample values.
     */
    private SampleGenerate SelectOFTwo(SampleGenerate A, SampleGenerate B, int n) {
        double sampleA = 0; // Total samples for alternative A
        double sampleB = 0; // Total samples for alternative B

        // Generate n samples for each alternative and accumulate results
        for (int i = 0; i < n; i++) {
            sampleA += A.runSystem();
            sampleB += B.runSystem();
        }

        // Compare total samples and return the better alternative
        if (sampleA >= sampleB) {
            A.setN(B.getN() + B.getNum() + A.getN());
            A.setSimulateTime(A.getSimulateTime() + B.getSimulateTime());
            return A;
        } else {
            B.setN(A.getN() + A.getNum() + B.getN());
            B.setSimulateTime(A.getSimulateTime() + B.getSimulateTime());
            return B;
        }
    }

    /**
     * Sorts an array and returns an array of indices representing the sorted order.
     *
     * @param keys Array of values to be sorted.
     * @return Array of indices in the sorted order of values in keys.
     */
    private int[] quickSort(double[] keys) {
        int[] indices = new int[keys.length];
        for (int i = 0; i < keys.length; i++) {
            indices[i] = i; // Initialize indices
        }
        quickSort(keys, 0, keys.length - 1, indices); // Perform quicksort
        return indices;
    }

    /**
     * Recursive quicksort method to sort the array and update indices based on sorted order.
     *
     * @param keys Array of values to be sorted.
     * @param begin Starting index of the array segment to be sorted.
     * @param end Ending index of the array segment to be sorted.
     * @param indices Array of indices representing the sorted order.
     */
    private void quickSort(double[] keys, int begin, int end, int[] indices) {
        if (begin >= 0 && begin < keys.length && end >= 0 && end < keys.length && begin < end) {
            int i = begin, j = end;
            double vot = keys[i]; // Pivot value
            int temp = indices[i]; // Temporary index for the pivot
            while (i != j) {
                while (i < j && keys[j] >= vot) j--; // Move j leftwards
                if (i < j) {
                    keys[i] = keys[j]; // Swap values
                    indices[i] = indices[j]; // Swap indices
                    i++;
                }
                while (i < j && keys[i] <= vot) i++; // Move i rightwards
                if (i < j) {
                    keys[j] = keys[i]; // Swap values
                    indices[j] = indices[i]; // Swap indices
                    j--;
                }
            }
            keys[i] = vot; // Place pivot in its correct position
            indices[i] = temp; // Update index of the pivot
            quickSort(keys, begin, j - 1, indices); // Recursively sort left segment
            quickSort(keys, i + 1, end, indices); // Recursively sort right segment
        }
    }
}


