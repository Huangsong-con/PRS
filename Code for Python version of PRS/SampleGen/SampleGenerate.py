import time
from numba import jit
import numpy as np


#  The simulation function using specified parameters and a random seed.
@jit(nopython=True) # Applying JIT decorator (Optional)
def runSimulation(argsP,seedP):
    s1, s2, s3 = argsP[1], argsP[2], argsP[3]
    b2, b3 = argsP[4], argsP[5]
    np.random.seed(seedP)
    # Main simulation process starts
    ST = np.zeros((2050, 3))
    ET = np.zeros((2050, 3))
    for i in range(2050):
        ST[i, 0] = np.random.exponential(1 / s1)
        ST[i, 1] = np.random.exponential(1 / s2)
        ST[i, 2] = np.random.exponential(1 / s3)
    ET[0, 0] = ST[0, 0]
    ET[0, 1] = ET[0, 0] + ST[0, 1]
    ET[0, 2] = ET[0, 1] + ST[0, 2]
    for i in range(1, 2050):
        ET[i, 0] = ET[i - 1, 0] + ST[i, 0]
        ET[i, 1] = max(ET[i - 1, 1], ET[i, 0]) + ST[i, 1]
        ET[i, 2] = max(ET[i - 1, 2], ET[i, 1]) + ST[i, 2]
        if i >= b2:
            ET[i, 0] = max(ET[i, 0], ET[int(i - b2), 1])
        if i >= b3:
            ET[i, 1] = max(ET[i, 1], ET[int(i - b3), 2])
    # Main simulation process ends
    return (2050 - 2000) / (ET[-1, 2] - ET[2000 - 1, 2])


class SampleGenerate:
    """
    A class to generate and manage samples for a simulation process.
    """

    def __init__(self, args):
        """
        Initialize the SampleGenerator with the given arguments.

        Args:
            args (np.array): Array of arguments representing the sample parameters.
        """
        self.args = np.array(args)
        self.mean = 0.0
        self.num = 0
        self.id = int(args[0])
        self.N = 0
        self.s2 = 0
        self.batch = 0
        self.nMax = 0
        self.seed = 0
        self.estimate_simulation_time = 0.0
        self.simulation_time = 0.0
        self.random_state = np.random.RandomState()

    def get_args(self):
        """Returns the sample arguments."""
        return self.args

    def set_args(self, args):
        """Sets the sample arguments."""
        self.args = np.array(args)

    def get_mean(self):
        """Returns the mean value of the sample."""
        return self.mean

    def set_mean(self, mean):
        """Sets the mean value of the sample."""
        self.mean = mean

    def get_num(self):
        """Returns the number of observations collected for this sample."""
        return self.num

    def set_num(self, num):
        """Sets the number of observations collected for this sample."""
        self.num = num

    def get_s2(self):
        """Returns the variance of the sample."""
        return self.s2

    def set_s2(self, s2):
        """Sets the variance of the sample."""
        self.s2 = s2

    def get_N(self):
        """Returns the total sample size for the alternative."""
        return self.N

    def set_N(self, N):
        """Sets the total sample size for the alternative."""
        self.N = N

    def get_batch(self):
        """Returns the batch size for the sample."""
        return self.batch

    def set_batch(self, batch):
        """Sets the batch size for the sample."""
        self.batch = batch

    def get_nMax(self):
        """Returns the maximum number of observations for the sample."""
        return self.nMax

    def set_nMax(self, nMax):
        """Sets the maximum number of observations for the sample."""
        self.nMax = nMax

    def get_id(self):
        """Returns the ID of the sample."""
        return self.id

    def get_seed(self):
        """Returns the random seed used for this sample."""
        return self.seed

    def set_seed(self, seed):
        """
        Sets the random seed and updates the random state generator.

        Args:
            seed (int): Seed value for random number generation.
        """
        self.random_state = np.random.RandomState(seed)
        self.seed = seed

    def set_simulation_time(self, simulation_time):
        """
        Sets the total simulation time.

        Args:
            simulation_time (float): Total time taken for simulations.
        """
        self.simulation_time = simulation_time

    def get_simulation_time(self):
        """Returns the total simulation time."""
        return self.simulation_time

    def get_estimate_simulation_time(self):
        """Returns the estimated simulation time based on initial samples."""
        return self.estimate_simulation_time

    def run_system(self):
        """
        Run a single simulation and update mean and simulation time.

        Returns:
            float: Observation value from the simulation run.
        """
        start_time = time.time()
        seed = self.random_state.randint(0, np.iinfo(np.int32).max)
        observation = runSimulation(self.args, seed)
        # Update mean with new observation
        self.mean = (self.mean * self.num + observation) / (self.num + 1)
        self.num += 1
        self.simulation_time += time.time() - start_time
        return observation

    def run_system_with_stage0(self, n0):
        """
        Run initial simulations to estimate average time per simulation.

        Args:
            n0 (int): Number of initial runs to perform.

        Returns:
            float: Average time taken per simulation.
        """
        start_time = time.time()
        for i in range(n0):
            seed = self.random_state.randint(0, np.iinfo(np.int32).max)
            runSimulation(self.args, seed)
        self.estimate_simulation_time = (time.time() - start_time) / n0
        return self.estimate_simulation_time
