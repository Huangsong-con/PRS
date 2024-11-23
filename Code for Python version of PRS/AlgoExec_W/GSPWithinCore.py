import numpy as np


class GSPPlus:
    """
    A class to implement the GSPPlus simulation procedure for selecting the best alternative system.
    """

    def __init__(self, alt, params):
        """
        Initialize the GSPPlus object with a list of alternatives and simulation parameters.

        Args:
            alt (list): List of alternative systems to simulate and compare.
            params (list): List of parameters for the GSPPlus procedure, including:
                - n1: Initial number of samples for each alternative.
                - rMax: Maximum number of simulation rounds.
                - beta: Scaling factor for batch size.
                - Additional parameters as needed.
        """
        self.ALT = alt  # List of alternative systems
        self.param = params  # Parameters for the simulation procedure

    def getALT(self):
        """
        Get the list of alternative systems.

        Returns:
            list: The list of alternative systems.
        """
        return self.ALT

    def setALT(self, ALT):
        """
        Set the list of alternative systems.

        Args:
            ALT (list): The new list of alternative systems.
        """
        self.ALT = ALT

    def getParam(self):
        """
        Get the simulation parameters.

        Returns:
            list: The list of parameters for the simulation.
        """
        return self.param

    def setParam(self, param):
        """
        Set the simulation parameters.

        Args:
            param (list): The new list of parameters for the simulation.
        """
        self.param = param

    def stage1Simulate(self):
        """
        Perform stage 1 simulation, collecting initial samples and calculating variance.

        Returns:
            float: The sum of square roots of the variances of the alternatives.
        """
        n1 = int(self.param[0])  # Initial number of samples for each alternative
        sumS = 0  # Variable to store the sum of square roots of variances
        s2 = np.zeros(len(self.ALT))  # Array to store variances of each alternative

        # Collect n1 samples for each alternative and calculate their means
        for i in range(len(self.ALT)):
            samples = np.zeros(n1)  # Array to store samples for each alternative
            mean = 0 # Mean of samples
            self.ALT[i].set_simulation_time(0)
            # Generate n1 samples for the current alternative
            for j in range(n1):
                samples[j] = self.ALT[i].run_system()
                mean += samples[j]/n1
            # Calculate variance for each alternative and update the sum of square roots
            for j in range(n1):
                s2[i] += (samples[j] - mean) ** 2 / (n1 - 1)
            self.ALT[i].set_s2(s2[i])  # Set the variance for each alternative
            sumS += np.sqrt(s2[i])  # Accumulate the sum of square roots of variances

        return sumS

    def set_batch_max(self, sumS):
        """
        Set the batch size and maximum number of simulations for each alternative.

        Args:
            sumS (float): Sum of square roots of the variances of the alternatives.
        """
        n1 = int(self.param[0])  # Initial number of samples
        rMax = int(self.param[1])  # Maximum number of simulation rounds
        beta = int(self.param[2])  # Scaling factor for batch size
        k = int(self.param[5])  # Number of alternatives

        # Calculate and set the batch size and max simulations for each alternative
        for i in range(len(self.ALT)):
            batch = int(np.ceil(beta * (np.sqrt(self.ALT[i].get_s2()) / (sumS / k))))
            self.ALT[i].set_batch(batch)
            self.ALT[i].set_nMax(n1 + rMax * batch)

    def screen(self, eta):
        """
        Screen alternatives based on the eta parameter, eliminating inferior ones.

        Args:
            eta (float): Eta value for screening, used to set the comparison threshold.

        Returns:
            list: The list of alternatives that pass the screening.
        """
        alt = []  # List to store alternatives that pass the screening
        countN = 0  # Count of total samples used
        n1 = int(self.param[0])  # Initial number of samples
        num = len(self.ALT)  # Number of alternatives
        elimination = np.zeros(num, dtype=int)  # Array to track eliminations
        simulate_time = 0  # Total simulation time
        mean = []  # List to store means of alternatives
        c1 = []  # Variance estimate for current samples
        c2 = []  # Variance estimate for maximum samples

        # Perform pairwise comparisons to mark alternatives for elimination
        for i in range(num):
            mean.append(self.ALT[i].get_mean())
            c1.append(self.ALT[i].get_s2() / self.ALT[i].get_num())
            c2.append(self.ALT[i].get_s2() / self.ALT[i].get_nMax())
            for j in range(i):
                y = (mean[i] - mean[j]) / (c1[i] + c1[j])
                a = eta * np.sqrt((n1 - 1) / (c2[i] + c2[j]))
                if y < -a:
                    elimination[i] += 1
                if -y < -a:
                    elimination[j] += 1

        # Collect alternatives that are not eliminated
        for i in range(num - 1, -1, -1):
            if elimination[i] < 1:
                alt.append(self.ALT[i])
            else:
                countN += (self.ALT[i].get_N() + self.ALT[i].get_num())
                simulate_time += self.ALT[i].get_simulation_time()

        # Update the total sample count and simulation time of the first passing alternative
        if alt:
            alt[0].set_N(countN + alt[0].get_N())
            alt[0].set_simulation_time(simulate_time + alt[0].get_simulation_time())
        return alt

    def simulate(self):
        """
        Perform simulations for each alternative based on its batch size.
        """
        for sample_generate in self.ALT:
            for j in range(sample_generate.get_batch()):
                sample_generate.run_system()

    def stage3Simulate(self, h):
        """
        Perform stage 3 simulation to finalize the best alternative.

        Args:
            h (float): Parameter used to calculate the required number of additional samples.

        Returns:
            The best alternative after stage 3 simulation.
        """
        N = 0  # Total number of samples used
        simulate_time = 0  # Total simulation time
        mean = np.zeros(len(self.ALT))  # Array to store means of alternatives

        # Perform additional simulations if needed and update statistics
        for i in range(len(self.ALT)):
            addN = max(0,
                       np.ceil(h * h * self.ALT[i].get_s2() / (self.param[6] * self.param[6])) - self.ALT[i].get_num())
            for j in range(int(addN)):
                self.ALT[i].run_system()
            N += (self.ALT[i].get_N() + self.ALT[i].get_num())
            simulate_time += self.ALT[i].get_simulation_time()
            mean[i] = self.ALT[i].get_mean()

        # Identify the alternative with the highest mean
        maxIndex = np.argmax(mean)
        self.ALT[maxIndex].set_simulation_time(simulate_time)
        self.ALT[maxIndex].set_N(N)
        return self.ALT[maxIndex]
