import time
import numpy as np
from pyspark import SparkConf, SparkContext
from Utilities.EtaFunc import EtaFunc
from AlgoExec_W.GSPWithinCore import GSPPlus
from Utilities.Rinott import rinott
from SampleGen.SampleGenerate import SampleGenerate


def stage1_simulation(alt, params):
    """
    Perform stage 1 simulation using the GSPPlus algorithm.

    Args:
        alt (list[SampleGenerate]): List of alternative systems to be simulated.
        params (array[float]): Parameters required for the GSPPlus simulation.

    Returns:
        tuple: A tuple containing:
            - alt (list[SampleGenerator]): The updated list of alternatives after simulation.
            - sumS (float): The sum of square roots of the variances from the stage 1 simulation.
    """
    stage1_sim = GSPPlus(alt, params)  # Initialize GSPPlus with alternatives and parameters
    sumS = stage1_sim.stage1Simulate()  # Perform the stage 1 simulation and calculate sumS
    return alt, sumS


def stage1_screen(alt, params, sumS, eta):
    """
    Perform stage 1 screening using the GSPPlus algorithm.

    Args:
        alt (list[SampleGenerate]): List of alternative systems to be screened.
        params (array[float]): Parameters required for the GSPPlus screening.
        sumS (float): The sum of square roots of the variances from the stage 1 simulation.
        eta (float): The threshold value used for screening alternatives.

    Returns:
        list[SampleGenerator]: The list of alternatives that pass the stage 1 screening.
    """
    stage1 = GSPPlus(alt, params)  # Initialize GSPPlus with alternatives and parameters
    stage1.set_batch_max(sumS)  # Set the maximum batch size based on the sumS value
    alt = stage1.screen(eta)  # Perform screening based on the eta threshold
    return alt


def stage2_simulation(alt, params):
    """
    Perform stage 2 simulation using the GSPPlus algorithm.

    Args:
        alt (list[SampleGenerate]): List of alternative systems to be simulated.
        params (array[float]): Parameters required for the GSPPlus simulation.

    Returns:
        list[SampleGenerator]: The updated list of alternatives after the stage 2 simulation.
    """
    stage2_sim = GSPPlus(alt, params)  # Initialize GSPPlus with alternatives and parameters
    stage2_sim.simulate()  # Perform the stage 2 simulation
    return alt


def stage2_screen(alt, params, eta):
    """
    Perform stage 2 screening using the GSPPlus algorithm.

    Args:
        alt (list[SampleGenerate]): List of alternative systems to be screened.
        params (array[float]): Parameters required for the GSPPlus screening.
        eta (float): The threshold value used for screening alternatives in stage 2.

    Returns:
        list[SampleGenerator]: The list of alternatives that pass the stage 2 screening.
    """
    stage2 = GSPPlus(alt, params)  # Initialize GSPPlus with alternatives and parameters
    alt_survivor = stage2.screen(eta)  # Perform screening based on the eta threshold
    return alt_survivor


# Function to perform stage 3 simulation using the GSPPlus algorithm
def stage3_simulation(alt, params, h):
    """
    Perform stage 3 simulation using the GSPPlus algorithm to select the best alternative.

    Args:
        alt (list[SampleGenerate]): List of alternative systems to be simulated.
        params (array[float]): Parameters required for the GSPPlus simulation.
        h (float): The Rinott constant used to determine the number of additional samples needed.

    Returns:
        SampleGenerate: The best alternative system after the final stage of simulation.
    """
    stage3_simulate = GSPPlus(alt, params)  # Initialize GSPPlus with alternatives and parameters
    alt = stage3_simulate.stage3Simulate(h)  # Perform the stage 3 simulation and select the best alternative
    return alt


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
        x (SampleGenerate): First alternative.
        y (SampleGenerate): Second alternative.

    Returns:
        SampleGenerate: The alternative with the higher mean value, with combined sample size and simulation time.
    """
    if x.get_mean() > y.get_mean():
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

    # Use the appropriate sample generation function based on seed
    return sample_param.map(lambda line: generate_sample_with_seed(line, random_seed))



def run_gsp_process(n1, rMax, beta, alpha1, alpha2, delta, nCores, parameters_path, n_alternative, seed=1234, repeat=1):
    """
    Main function to run the GSP (Generalized Selection Procedure) algorithm with the specified parameters.

    Args:
        n1 (int): Initial number of samples for each alternative.
        rMax (int): Maximum number of rounds for stage 2 simulations.
        beta (float): Scaling factor for batch size.
        alpha1 (float): Significance level for the first stage of screening.
        alpha2 (float): Significance level for the final stage of screening.
        delta (float): Indifference zone parameter.
        nCores (int): Number of CPU cores to use for parallel processing.
        parameters_path (str): Path to the input data file containing alternatives and parameters.
        n_alternative (int): Total number of alternatives being considered.
        seed (int, optional): Random seed for reproducibility. Defaults to -1 (no fixed seed).
        repeat (int, optional): Number of times to repeat the GSP process. Defaults to 1.

    Returns:
        list: A list of tuples containing the results of each replication, where each tuple includes:
              - Replication number (int)
              - ID of the best alternative (int)
              - Wall clock time (float)
              - Total simulation time for the best alternative (float)
    """
    # Initialize Spark configuration and create or get the SparkContext
    conf = SparkConf().setAppName("GSP").set("spark.cores.max", str(nCores))
    sc = SparkContext.getOrCreate(conf)
    param = np.array([n1, rMax, beta, alpha1, alpha2, n_alternative, delta])  # Parameters for GSP
    results = []  # List to store the results of each replication

    # Initialize random seed generator
    random_repeat_seed = np.random.RandomState(seed)


    def run_gsp(repeat_seed, replication):
        """
        Inner function to execute the GSP algorithm for a single replication.

        Args:
            repeat_seed (int): Seed for random number generation.
            replication (int): Replication number for tracking purposes.
        """
        # Load input data and generate samples
        sample_param = sc.textFile(parameters_path, nCores)
        sample = generate_sample(sample_param, repeat_seed)
        uniform_sample_key = sample.partitionBy(nCores)  # Partition samples across cores
        uniform_sample = uniform_sample_key.map(lambda x: x[1]).glom().map(lambda x: list(x))

        print("Enter the stage1!")
        # Perform stage 1 simulation
        stage1_sim = uniform_sample.map(lambda x: stage1_simulation(x, param))
        sumS = stage1_sim.mapPartitions(lambda partition: [sum(x[1] for x in partition)]).reduce(lambda a, b: a + b)

        # Perform stage 1 screening
        stage1_scr = stage1_sim.map(
            lambda x: stage1_screen(x[0], param, sumS, EtaFunc.find_eta(n1, alpha1, n_alternative))
        )
        screen_result = stage1_scr.flatMap(lambda x: x)  # Collect results from stage 1 screening

        r = 0  # Initialize stage counter
        print("Enter the stage2!")
        while r < rMax:
            elements = screen_result.take(2)  # Take the first two elements
            if len(elements) == 1:
                # If only one alternative remains, record the result
                best_alt = elements[0]
                best_args = best_alt.get_args()
                clock_time = time.time() - start_time  # Calculate wall clock time
                total_simulated_time = best_alt.get_simulation_time()

                # Store the result
                results.append((replication + 1, int(best_args[0]), clock_time, total_simulated_time))
                break

            # Sort and redistribute screened alternatives across cores
            screen_result_sort = screen_result.map(lambda x: (x.get_id(), x)).sortByKey()
            screen_result_reID = screen_result_sort.zipWithIndex().map(
                lambda x: (x[1] % nCores, x[0][1])
            )
            uniform_screen_result = (screen_result_reID.partitionBy(nCores)
                                     .map(lambda x: x[1]).glom().map(lambda x: list(x)))

            # Perform stage 2 simulation and screening
            stage2_sim = uniform_screen_result.map(lambda x: stage2_simulation(x, param))
            stage2_scr = stage2_sim.map(
                lambda x: stage2_screen(x, param, EtaFunc.find_eta(n1, alpha1, n_alternative))
            )
            r += 1  # Increment the stage counter

            if r == rMax:
                print("Enter the stage3!")
                # Perform stage 3 simulation to find the best alternative
                uniform_stage3 = stage2_scr.flatMap(lambda x: x).repartition(nCores).glom().map(
                    lambda x: list(x)).filter(lambda x: x)
                stage3_ss = uniform_stage3.map(
                    lambda x: stage3_simulation(x, param, rinott(n_alternative, 1 - alpha2, n1 - 1))
                )
                best_alt = stage3_ss.reduce(lambda x, y: reduce_logic(x, y))
                best_args = best_alt.get_args()
                clock_time = time.time() - start_time
                total_simulated_time = best_alt.get_simulation_time()

                # Store the result
                results.append((replication + 1, int(best_args[0]), clock_time, total_simulated_time))

            # Update the alternatives for the next round
            screen_result = stage2_scr.flatMap(lambda x: x)

    # Run the GSP process for the specified number of repetitions
    for _ in range(repeat):
        start_time = time.time()  # Record start time
        run_gsp(random_repeat_seed.randint(0, np.iinfo(np.int32).max), _)  # Run the GSP process

    sc.stop()  # Stop the SparkContext
    return results  # Return the list of results


def generate_sample_with_stage0(sample_param, random_seed, n):
    """
    Generates samples with an initial stage (stage0), optionally using a random seed for reproducibility.

    Args:
        sample_param: An iterable (e.g., RDD or list) containing lines of data, where each line is a space-separated
                      string with an ID followed by parameters.
        random_seed: An integer seed used for random number generation to ensure reproducibility.
        n: An integer representing the number of iterations or samples to run in the system with stage0.

    Returns:
        An iterable (e.g., RDD or list) of tuples, where each tuple contains:
            - alt: The generated sample (alternative).
            - estimate_sim_time: The estimated simulation time when running the system with stage0.
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

    def generate_sample_stage0(line, number, in_seed):
        """
        Generates a sample without using a specific seed and runs the system with stage0.

        Args:
            line: A string of input data to parse and generate a sample from.
            number: The number of iterations or samples to run in the system with stage0.
            in_seed: An integer seed to initialize the random number generator.

        Returns:
            A tuple containing:
                - alt: The generated sample (alternative) with a set seed.
                - estimate_sim_time: The estimated simulation time when running the system with stage0.
        """
        ID, params = parse_line(line)
        alt = sample_generation(params)  # Generate the sample (alternative) without a seed
        estimate_sim_time = alt.run_system_with_stage0(number)  # Run the system with stage0
        random = np.random.RandomState(in_seed + ID)  # Initialize a random generator with a unique seed
        alt_seed = random.randint(0, np.iinfo(np.int32).max)  # Generate an alternative seed within the int32 range
        alt.set_seed(alt_seed)  # Set the alternative seed
        return alt, estimate_sim_time  # Return the sample and its estimated simulation time

    # Use the appropriate sample generation function based on whether a seed should be used
    return sample_param.map(lambda line: generate_sample_stage0(line, n,random_seed))


def run_gsp_process_with_stage0(n0, n1, rMax, beta, alpha1, alpha2, delta, nCores, parameters_path, n_alternative,
                                seed=-1234, repeat=1):
    """
    Main function to run the GSP (Generalized Selection Procedure) algorithm with initial stage 0 sampling.

    Args:
        n0 (int): Number of initial samples for stage 0.
        n1 (int): Number of samples for each alternative in stage 1.
        rMax (int): Maximum number of rounds for stage 2 simulations.
        beta (float): Scaling factor for batch size.
        alpha1 (float): Significance level for stage 1 screening.
        alpha2 (float): Significance level for the final stage of screening.
        delta (float): Indifference zone parameter.
        nCores (int): Number of CPU cores to use for parallel processing.
        parameters_path (str): Path to the input data file containing alternatives and parameters.
        n_alternative (int): Total number of alternatives being considered.
        seed (int, optional): Random seed for reproducibility. Defaults to 1234.
        repeat (int, optional): Number of times to repeat the GSP process. Defaults to 1.

    Returns:
        list: A list of tuples containing the results of each replication, where each tuple includes:
              - Replication number (int)
              - ID of the best alternative (int)
              - Wall clock time (float)
              - Total simulation time for the best alternative (float)
    """
    # Initialize Spark configuration and create or get the SparkContext
    conf = SparkConf().setAppName("GSP").set("spark.cores.max", str(nCores))
    sc = SparkContext.getOrCreate(conf)
    param = np.array([n1, rMax, beta, alpha1, alpha2, n_alternative, delta])  # Parameters for GSP
    results = []  # List to store the results of each replication

    # Initialize random seed generator
    random_repeat_seed = np.random.RandomState(seed)

    def repartition_by_balanced_value(rdd, num_partitions):
        """
        Repartition an RDD of (SampleGenerate, Double) tuples to balance the values across partitions.

        Args:
            rdd (RDD): Input RDD containing (SampleGenerate, Double) tuples.
            num_partitions (int): Number of partitions for the output RDD.

        Returns:
            RDD: A repartitioned RDD with balanced value sums across partitions.
        """
        # Collect and sort non-empty data in descending order by value
        non_empty_data = rdd.filter(lambda x: x is not None).collect()
        non_empty_data.sort(key=lambda x: -x[1])

        # Initialize partitions and their respective value sums
        partition_sums = [0.0] * num_partitions
        partitions = [[] for _ in range(num_partitions)]

        # Distribute data to partitions to balance value sums
        for key, value in non_empty_data:
            min_partition_index = partition_sums.index(min(partition_sums))
            partitions[min_partition_index].append((key, value))
            partition_sums[min_partition_index] += value

        # Flatten the list of partitions and create a balanced RDD
        partitioned_data = [item for sublist in partitions for item in sublist]
        balanced_rdd = rdd.context.parallelize(partitioned_data, num_partitions)

        return balanced_rdd

    def run_gsp(repeat_seed, replication):
        """
        Inner function to execute the GSP algorithm for a single replication.

        Args:
            repeat_seed (int): Seed for random number generation.
            replication (int): Replication number for tracking purposes.
        """
        # Load input data and generate initial samples with stage 0
        sample_param = sc.textFile(parameters_path, nCores)
        sample = generate_sample_with_stage0(sample_param, repeat_seed, n0)

        # Repartition samples to balance values across partitions
        uniform_sample_key = repartition_by_balanced_value(sample, nCores)
        uniform_sample = uniform_sample_key.map(lambda x: x[0]).glom().map(lambda x: list(x))

        print("Enter the stage1!")
        # Perform stage 1 simulation
        stage1_sim = uniform_sample.map(lambda x: stage1_simulation(x, param))
        sumS = stage1_sim.map(lambda x: x[1]).reduce(lambda a, b: a + b)

        # Perform stage 1 screening
        stage1_scr = stage1_sim.map(
            lambda x: stage1_screen(x[0], param, sumS, EtaFunc.find_eta(n1, alpha1, n_alternative))
        )
        screen_result = stage1_scr.flatMap(lambda x: x)

        print("Enter the stage2!")
        r = 0  # Stage counter for stage 2 simulations
        while r < rMax:
            elements = screen_result.take(2)
            if len(elements) == 1:
                # If only one alternative remains, record the result
                best_alt = elements[0]
                best_args = best_alt.get_args()
                clock_time = time.time() - start_time  # Calculate wall clock time
                total_simulated_time = best_alt.get_simulation_time()

                # Store the result
                results.append((replication + 1, int(best_args[0]), clock_time, total_simulated_time))
                break

            # Repartition screened results for stage 2
            screen_result_rdd = screen_result.map(lambda x: (x, x.get_estimate_simulation_time()))
            uniform_screen_result = (repartition_by_balanced_value(screen_result_rdd, nCores)
                                     .map(lambda x: x[0]).glom().map(lambda x: list(x)))

            # Perform stage 2 simulation and screening
            stage2_sim = uniform_screen_result.map(lambda x: stage2_simulation(x, param))
            stage2_scr = stage2_sim.map(
                lambda x: stage2_screen(x, param, EtaFunc.find_eta(n1, alpha1, n_alternative))
            )
            r += 1  # Increment the stage counter

            if r == rMax:
                print("Enter the stage3!")
                # Perform stage 3 simulation to select the best alternative
                uniform_stage3 = stage2_scr.flatMap(lambda x: x).map(lambda x: (x, x.get_estimate_simulation_time()))
                uniform_stage3 = repartition_by_balanced_value(uniform_stage3, nCores)
                uniform_stage3_list = uniform_stage3.map(lambda x: x[0]).glom().map(lambda x: list(x))

                stage3_ss = uniform_stage3_list.map(
                    lambda x: stage3_simulation(x, param, rinott(n_alternative, 1 - alpha2, n1 - 1))
                )
                best_alt = stage3_ss.reduce(lambda x, y: reduce_logic(x, y))
                best_args = best_alt.get_args()
                clock_time = time.time() - start_time  # Calculate wall clock time
                total_simulated_time = best_alt.get_simulation_time()

                # Store the result
                results.append((replication + 1, int(best_args[0]), clock_time, total_simulated_time))

            # Update alternatives for the next round
            screen_result = stage2_scr.flatMap(lambda x: x)

    # Run the GSP process for the specified number of repetitions
    for _ in range(repeat):
        start_time = time.time()
        run_gsp(random_repeat_seed.randint(0, np.iinfo(np.int32).max), _)

    sc.stop()  # Stop the SparkContext
    return results  # Return the list of results
