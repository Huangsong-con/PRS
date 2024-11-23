import numpy as np
from pyspark import SparkConf, SparkContext
import time
from AlgoExec_W.KTWithinCore import KTPlus
from SampleGen.SampleGenerate import SampleGenerate


def select(ALT, params):
    """
    Select the best alternative using the KTPlus algorithm.

    Args:
        ALT (list[SampleGenerate]): List of alternatives to be evaluated.
        params (list[float]): Parameters required by the KTPlus algorithm.

    Returns:
        SampleGenerate: The best alternative based on the KTPlus selection.
    """
    selection = KTPlus(ALT, params)  # Initialize KTPlus with alternatives and parameters
    selection.KTSelect()  # Perform KT selection
    return selection.get_best_alt()  # Return the best alternative


def sample_generation(params):
    """
    Generate a sample using the given parameters and random state.

    Args:
        params (np.array): Parameters required to generate the sample.

    Returns:
        SampleGenerate: An instance of SampleGenerate with the generated sample.
    """
    alt = SampleGenerate(params)  # Generate sample
    return alt


def reduce_logic(x, y):
    """
    Reduce two alternatives to one based on their mean values.

    Args:
        x (SampleGenerate): The first alternative.
        y (SampleGenerate): The second alternative.

    Returns:
        SampleGenerate: The alternative with the higher mean value, with combined sample size and simulation time.
    """
    if x.get_mean() > y.get_mean():  # Compare mean values
        x.set_N(x.get_N() + y.get_N())  # Combine the sample sizes
        x.set_simulation_time(x.get_simulation_time() + y.get_simulation_time())  # Combine simulation times
        return x
    else:
        y.set_N(x.get_N() + y.get_N())  # Combine the sample sizes
        y.set_simulation_time(y.get_simulation_time() + x.get_simulation_time())  # Combine simulation times
        return y


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


def run_kt_process(alpha, delta, n0, g, nCores, parameters_path, seed=1234, repeat=1):
    """
    Run the KTPlus process using the specified parameters and Spark for parallel processing.

    Args:
        alpha (float): The significance level for the KT algorithm.
        delta (float): The indifference zone parameter, representing the minimum difference to be detected.
        n0 (int): The initial number of samples to collect for each alternative.
        g (int): The group size for the KT algorithm, used for partitioning.
        nCores (int): The number of CPU cores to use for parallel processing.
        parameters_path (str): The file path to the input data containing alternatives and parameters.
        seed (int, optional): The random seed for reproducibility. Defaults to -1, indicating no fixed seed.
        repeat (int, optional): The number of times to repeat the process for multiple replications. Defaults to 1.

    Returns:
        results (list): A list of tuples, where each tuple contains:
            - Replication number (int)
            - ID of the best alternative (int)
            - Wall clock time taken for the process (float)
            - Total simulation time for the best alternative (float)
    """
    # Initialize Spark configuration and create or get the SparkContext
    conf = SparkConf().setAppName("KT").set("spark.cores.max", str(nCores))
    sc = SparkContext.getOrCreate(conf)  # Get or create a SparkContext
    params = [alpha, delta, n0, g, nCores]  # List of parameters to pass to the KT selection process
    results = []  # List to store the results of each replication

    # Initialize the random seed generator
    random_repeat_seed = np.random.RandomState(seed)  # Use the provided seed for reproducibility


    def run_kt(repeat_seed, replication):
        """
        Run the KT selection process for a single replication.

        Args:
            repeat_seed (int): The random seed for this replication.
            replication (int): The replication number for tracking purposes.
        """
        # Load input data using Spark and partition it across the specified number of cores
        sample_param = sc.textFile(parameters_path, nCores)
        sample = generate_sample(sample_param, repeat_seed)  # Generate samples using the provided seed

        # Partition and cache the samples for efficient processing
        uniform_sample_key = sample.partitionBy(nCores)
        uniform_sample = uniform_sample_key.map(lambda x: x[1]).glom().map(lambda x: list(x)).cache()

        # Perform the KT selection process on each partition and cache the results
        best_alt_core = uniform_sample.map(lambda ALT: select(ALT, params)).cache()
        best_alt = best_alt_core.reduce(lambda x, y: reduce_logic(x, y))  # Reduce to find the overall best alternative

        # Extract the arguments and metrics of the best alternative
        best_args = best_alt.get_args()
        clock_time = time.time() - start_time  # Calculate the elapsed wall clock time
        total_simulated_time = best_alt.get_simulation_time()  # Get the total simulation time

        # Append the results for this replication
        results.append((replication + 1, int(best_args[0]), clock_time, total_simulated_time))

    # Run the KT process for the specified number of repetitions
    for _ in range(repeat):
        start_time = time.time()  # Record the start time for measuring wall clock time
        run_kt(random_repeat_seed.randint(0, np.iinfo(np.int32).max), _)  # Run with a new random seed each time

    sc.stop()  # Stop the SparkContext to release resources
    return results  # Return the list of results
