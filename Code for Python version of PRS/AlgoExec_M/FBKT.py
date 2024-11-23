import time
import numpy as np
from pyspark import SparkConf, SparkContext
from AlgoExec_W.FBKTWithinCore import FBKTPlus
from SampleGen.SampleGenerate import SampleGenerate


def select(ALT, Params):
    """
    Perform selection of the best alternative using the FBKTPlus algorithm.

    Args:
        ALT (list): List of alternative systems to be evaluated.
        Params (list): Parameters required for the FBKTPlus selection procedure.

    Returns:
        SampleGenerate: The best alternative system selected by the FBKTPlus algorithm.
    """
    selection = FBKTPlus(ALT, Params)  # Initialize FBKTPlus with alternatives and parameters
    selection.FBKTselect()  # Execute the FBKT selection procedure
    return selection.get_best_alt()  # Return the best alternative identified


def sample_generation(params):
    """
    Generate a sample based on the provided parameters.

    Args:
        params (np.array): Parameters required to generate the sample.

    Returns:
        SampleGenerate: An instance of SampleGenerator containing the generated alternative.
    """
    alt = SampleGenerate(params)  # Create a new SampleGenerator instance using the given parameters
    return alt


def reduce_logic(x, y):
    """
    Combine two SampleGenerator objects based on their mean values and simulation data.

    Args:
        x (SampleGenerate): The first SampleGenerator object to compare.
        y (SampleGenerate): The second SampleGenerator object to compare.

    Returns:
        SampleGenerate: The SampleGenerator object with the higher mean value,
                         updated with the combined sample size and simulation time.
    """
    if x.get_mean() > y.get_mean():  # Compare mean values of x and y
        x.set_N(x.get_N() + y.get_N())  # Add the sample sizes of x and y
        x.set_simulation_time(x.get_simulation_time() + y.get_simulation_time())  # Add the simulation times
        return x  # Return x if it has the higher mean
    else:
        y.set_N(x.get_N() + y.get_N())  # Add the sample sizes of y and x
        y.set_simulation_time(y.get_simulation_time() + x.get_simulation_time())  # Add the simulation times
        return y  # Return y if it has the higher mean


def generate_sample(sample_param, random_seed):
    """
    Generates samples based on input parameters, with or without a random seed.

    Args:
        sample_param: An iterable (e.g., RDD or list) containing lines of data, where each line is a space-separated
                      string with an ID followed by parameters.
        random_seed: An integer seed used for random number generation to ensure reproducibility.

    Returns:
        An iterable (e.g., RDD or list) of tuples, where each tuple contains an ID and a generated alternative.
    """

    def parse_line(line):
        """
        Parses a line of input data to extract the ID and parameters.

        Args:
            line: A string where the first element is the ID and the rest are parameters, space-separated.

        Returns:
            A tuple containing:
                - ID: An integer representing the ID.
                - params: A NumPy array of floats representing the parameters.
        """
        parts = line.split(" ")
        ID = int(parts[0])  # Extract ID as an integer
        params = np.array(list(map(float, parts)))  # Convert the rest of the line to a NumPy array of floats
        return ID, params

    def generate_sample_with_seed(line, in_seed):
        """
        Generates a sample using a random seed for reproducibility.

        Args:
            line: A string of input data to parse and generate a sample from.
            in_seed: An integer seed to initialize the random number generator.

        Returns:
            A tuple containing:
                - ID: An integer representing the ID.
                - alt: The generated alternative using seeded random number generation.
        """
        ID, params = parse_line(line)
        random = np.random.RandomState(in_seed + ID)  # Initialize a random generator with a unique seed
        alt_seed = random.randint(0, np.iinfo(np.int32).max)  # Generate an alternative seed within the int32 range
        alt = sample_generation(params)  # Generate the alternative
        alt.set_seed(alt_seed)  # Set the alternative seed
        return ID, alt  # Return the ID and generated alternative

    # Use the appropriate sample generation function based on whether a seed should be used
    return sample_param.map(lambda line: generate_sample_with_seed(line, random_seed))


def run_fbkt_process(N, n0, phi, nCores, parameters_path, seed=1234, repeat=1):
    """
    Main function to run the FBKTPlus algorithm with the specified parameters.

    Args:
        N (int): Total sample budget to be allocated across cores.
        n0 (int): Initial number of samples for each alternative.
        phi (int): Parameter that determines budget allocation in the FBKTPlus algorithm.
        nCores (int): Number of CPU cores to use for parallel processing.
        parameters_path (str): Path to the input data file containing alternative parameters.
        seed (int, optional): Random seed for reproducibility. Defaults to -1 (no fixed seed).
        repeat (int, optional): Number of times to repeat the FBKT process. Defaults to 1.

    Returns:
        list: A list of tuples containing the results of each replication, where each tuple includes:
              - Replication number (int)
              - ID of the best alternative (int)
              - Wall clock time (float)
              - Total simulation time for the best alternative (float)
    """
    # Initialize Spark configuration and create or get the SparkContext
    conf = SparkConf().setAppName("FBKT").set("spark.cores.max", str(nCores))
    sc = SparkContext.getOrCreate(conf)  # Get or create a SparkContext

    # Define parameters for the FBKTPlus algorithm
    params = [int(N / nCores), n0, phi]

    results = []  # List to store the results of each replication

    # Initialize random seed generator
    random_repeat_seed = np.random.RandomState(seed)

    def run_fbkt(repeat_seed, replication):
        """
        Inner function to execute the FBKTPlus algorithm for a single replication.

        Args:
            repeat_seed (int): Seed for random number generation.
            replication (int): Replication number for tracking purposes.
        """
        # Load input data and generate samples
        sample_param = sc.textFile(parameters_path, nCores)
        sample = generate_sample(sample_param, repeat_seed)

        # Partition the samples and group them for processing
        uniform_sample_key = sample.partitionBy(nCores)
        uniform_sample = uniform_sample_key.map(lambda x: x[1]).glom().map(lambda x: list(x))

        # Perform FBKT selection on each partition and reduce to find the best alternative
        best_alt_core = uniform_sample.map(lambda ALT: select(ALT, params))
        best_alt = best_alt_core.reduce(lambda x, y: reduce_logic(x, y))

        # Get details of the best alternative
        best_args = best_alt.get_args()
        clock_time = time.time() - start_time  # Calculate wall clock time
        total_simulation_time = best_alt.get_simulation_time()  # Get the total simulation time

        # Store the results of the current replication
        results.append((replication + 1, int(best_args[0]), clock_time, total_simulation_time))

    # Run the FBKTPlus algorithm for the specified number of repetitions
    for _ in range(repeat):
        start_time = time.time()  # Record start time
        run_fbkt(random_repeat_seed.randint(0, np.iinfo(np.int32).max), _)  # Run FBKT for the current repeat

    sc.stop()  # Stop the SparkContext after all repetitions are complete
    return results  # Return the list of results
