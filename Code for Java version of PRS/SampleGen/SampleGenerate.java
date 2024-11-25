package SampleGen;

import java.io.Serializable;
import java.util.Random;

/**
 * Class representing a sample generation process for alternatives.
 * This class contains methods to simulate an alternative and track various statistical properties.
 */
public class SampleGenerate implements Serializable {

    // Parameters of the alternative
    double[] args;
    // ID of the alternative
    int id;
    // Sample mean of the alternative
    double mean;
    // Number of samples of the alternative
    int num;
    // Total number of samples
    int N;
    // Sample variance of the alternative
    double s2;
    // Parameter of the GSP procedure
    int batch;
    // Parameter of the GSP procedure
    int nMax;
    // Random seed for the alternative
    long seed;
    // Estimated simulation time for stage 0
    double estimateSimTime;
    // Total simulation time used by the alternative
    double simulateTime;
    // Random number generator for the alternative
    Random random = new Random();

    // Getter for the parameters of the alternative
    public double[] getArgs() {
        return args;
    }

    // Getter for the sample mean
    public double getMean() {
        return mean;
    }

    // Setter for the sample mean
    public void setMean(double mean) {
        this.mean = mean;
    }

    // Getter for the number of samples
    public int getNum() {
        return num;
    }

    // Getter for the sample variance
    public double getS2() {
        return s2;
    }

    // Setter for the sample variance
    public void setS2(double s2) {
        this.s2 = s2;
    }

    // Getter for the total number of samples
    public int getN() {
        return N;
    }

    // Setter for the total number of samples
    public void setN(int n) {
        this.N = n;
    }

    // Getter for the batch size
    public int getBatch() {
        return batch;
    }

    // Setter for the batch size
    public void setBatch(int batch) {
        this.batch = batch;
    }

    // Getter for the ID of the alternative
    public int getId() {
        return id;
    }

    // Getter for the maximum number of samples
    public int getnMax() {
        return nMax;
    }

    // Setter for the maximum number of samples
    public void setnMax(int nMax) {
        this.nMax = nMax;
    }

    // Getter for the estimated simulation time for stage 0
    public double getEstimateSimTime() {
        return estimateSimTime;
    }

    // Setter for the estimated simulation time
    public void setEstimateSimTime(double estimateSimTime) {
        this.estimateSimTime = estimateSimTime;
    }

    // Getter for the total simulation time
    public double getSimulateTime() {
        return simulateTime;
    }

    // Setter for the total simulation time
    public void setSimulateTime(double simulateTime) {
        this.simulateTime = simulateTime;
    }

    // Set the seed for the random number generator
    public void setSeed(long seed) {
        random.setSeed(seed);
        this.seed = seed;
    }

    // Getter for the random seed
    public long getSeed() {
        return seed;
    }

    /**
     * Constructor to initialize the alternative with its parameters.
     *
     * @param args Parameters of the alternative.
     */
    public SampleGenerate(double[] args) {
        this.args = args;
        this.mean = 0.0;
        this.num = 0;
        this.id = (int) args[0];
    }

    /**
     * Method to run a simulation for the alternative with given parameters and seed.
     * This method simulates the process based on exponential distributions and calculates the throughput.
     *
     * @param argsJ Array of parameters for the simulation.
     * @param seedJ Random seed for the simulation.
     * @return The throughput calculated from the simulation.
     */

    public double runSimulation(double[] argsJ,long seedJ) {
        double s1 = argsJ[1];
        double s2 = argsJ[2];
        double s3 = argsJ[3];
        double b2 = argsJ[4];
        double b3 = argsJ[5];
        Random R = new Random(seedJ);
        //Main simulation process starts
        double[][] ST = new double[2050][3];
        double[][] ET = new double[2050][3];
        for (int i = 0; i < 2050; i++){
            ST[i][0] = -Math.log(1-R.nextDouble())/s1;
            ST[i][1] = -Math.log(1-R.nextDouble())/s2;
            ST[i][2] = -Math.log(1-R.nextDouble())/s3;
        }
        ET[0][0] = ST[0][0];
        ET[0][1] = ET[0][0] + ST[0][1];
        ET[0][2] = ET[0][1] + ST[0][2];
        for(int i=1; i < 2050; i++){
            ET[i][0] = ET[i - 1][0] + ST[i][0];
            ET[i][1] = Math.max(ET[i - 1][1], ET[i][0]) + ST[i][1];
            ET[i][2] = Math.max(ET[i - 1][2], ET[i][1]) + ST[i][2];
            if (i >= b2) {
                ET[i][0] = Math.max(ET[i][0], ET[(int)(i - b2)][1]);
            }
            if (i >= b3) {
                ET[i][1] = Math.max(ET[i][1], ET[(int)(i - b3)][2]);
            }
        }
        //Main simulation process ends
        return (2050 - 2000) / (ET[2050 - 1][2] - ET[2000 - 1][2]);
    }

    /**
     * Method to run the system simulation and update statistical properties.
     *
     * @return The observation result from the simulation.
     */
    public double runSystem() {
        long startTime = System.nanoTime(); // Record the start time
        long runSeed = random.nextLong();
        double observation = runSimulation(args, runSeed);
        mean = (mean * num + observation) / (num + 1); // Update the sample mean
        num += 1; // Increment the sample count
        simulateTime += (System.nanoTime() - startTime) / 1e9; // Update simulation time in seconds
        return observation;
    }

    /**
     * Method to run the system simulation with stage 0 and estimate simulation time.
     *
     * @param n0 Number of initial simulations to run.
     * @return The estimated simulation time.
     */
    public double runSystemWithStage0(int n0) {
        long startTime = System.nanoTime(); // Record the start time
        for (int i = 0; i < n0; i++) {
            long runSeed = random.nextLong();
            runSimulation(args, runSeed);
        }
        estimateSimTime = (System.nanoTime() - startTime) / (n0 * 1e9); // Estimate simulation time per run in seconds
        return estimateSimTime;
    }
}
