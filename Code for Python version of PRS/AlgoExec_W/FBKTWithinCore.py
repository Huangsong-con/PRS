import math
import numpy as np


def select_of_two(A, B, n):
    """
    Select the better of two alternatives A and B based on the sum of n simulations.

    Parameters:
    A: First alternative.
    B: Second alternative.
    n: Number of simulations to run for each alternative.

    Returns:
    The better alternative with updated statistics.
    """
    # Run n simulations and sum the results for each alternative
    sampleA = sum(A.run_system() for _ in range(n))
    sampleB = sum(B.run_system() for _ in range(n))

    # Compare the sums and select the better alternative
    if sampleA >= sampleB:
        A.set_N(A.get_N() + B.get_N() + B.get_num())
        A.set_simulation_time(A.get_simulation_time() + B.get_simulation_time())
        return A
    else:
        B.set_N(B.get_N() + A.get_N() + A.get_num())
        B.set_simulation_time(B.get_simulation_time() + A.get_simulation_time())
        return B


class FBKTPlus:
    def __init__(self, ALT, parm):
        """
        Initialize the FBKTPlus object with a list of alternatives and parameters.

        Args:
            ALT (list): List of alternative systems to be compared.
            parm (list): List of parameters, where:
                         - parm[0] (int): Total budget for simulation runs (N).
                         - parm[1] (int): Initial number of simulations per alternative (n0).
                         - parm[2] (int): Parameter controlling the number of simulations (phi).
        """
        self.best_alt = None  # Variable to store the best alternative
        self.ALT = ALT  # List of alternatives
        self.parm = parm  # Parameters for the FBKT selection algorithm

    def get_best_alt(self):
        """
        Get the best alternative selected by the FBKT algorithm.

        Returns:
            SampleGenerator: The best alternative system.
        """
        return self.best_alt

    def FBKTselect(self):
        """
        Perform the FBKT selection algorithm to identify the best alternative.

        The algorithm runs in multiple rounds, where each round narrows down
        the list of alternatives based on their mean performance until one
        alternative remains as the best.
        """
        N = int(self.parm[0])  # Total budget for simulation runs
        n0 = int(self.parm[1])  # Initial number of simulations per alternative
        phi = int(self.parm[2])  # Parameter that influences the allocation of simulations
        alt_count = len(self.ALT)  # Number of alternatives
        R = math.ceil(math.log(alt_count) / math.log(2))  # Number of selection rounds needed

        # Initial stage: Run `n0` simulations for each alternative
        for alt in self.ALT:
            for _ in range(n0):
                alt.run_system()  # Perform simulation for the alternative

        # Copy the list of alternatives for the selection rounds
        Ir = self.ALT.copy()

        # Selection rounds to narrow down the alternatives
        for i in range(R):
            Ir1 = []  # List to hold selected alternatives for the next round
            means = [alt.get_mean() for alt in Ir]  # Calculate mean values of each alternative
            index = np.argsort(means)  # Sort alternatives by their mean values
            r = i + 1
            Nr = int(np.floor(r / ((phi - 1) * phi) * ((phi - 1) / phi) ** r * N))  # Calculate budget for the round
            Nl = int(np.floor(Nr / len(Ir)))  # Simulations per alternative for this round

            if len(Ir) % 2 == 0:  # If the number of alternatives is even
                for j in range(len(Ir) // 2):
                    # Pair up alternatives and select the better one
                    winner = select_of_two(Ir[index[j]], Ir[index[-1 - j]], Nl)
                    Ir1.append(winner)
            else:  # If the number of alternatives is odd
                Ir1.append(Ir[index[-1]])  # Pass the last alternative to the next round
                for j in range((len(Ir) - 1) // 2):
                    # Pair up alternatives and select the better one
                    winner = select_of_two(Ir[index[j]], Ir[index[-2 - j]], Nl)
                    Ir1.append(winner)
            Ir = Ir1  # Update the list of alternatives for the next round

        # Final round of simulations to refine the best alternative
        Nr1 = int(np.floor((R + 1) / ((phi - 1) * phi) * ((phi - 1) / phi) ** (R + 1) * N))
        mean = 0
        for _ in range(Nr1):
            mean += Ir[0].run_system() / Nr1  # Perform simulations and calculate the final mean
        Ir[0].set_mean(mean)  # Set the final mean for the best alternative
        Ir[0].set_N(Ir[0].get_N() + Ir[0].get_num())  # Update the total number of simulations for the best alternative
        self.best_alt = Ir[0]  # Set the best alternative
