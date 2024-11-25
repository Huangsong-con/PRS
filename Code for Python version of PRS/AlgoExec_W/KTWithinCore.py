import math
from Utilities.Rinott import rinott


def KN(ALT, n0, alpha, delta):
    """
    Executes the KN (Kim and Nelson) selection procedure to identify the best alternative system from a list.

    Args:
        ALT (list): List of alternative systems to be compared, where each system can run simulations and return
                    results and statistics.
        n0 (int): Initial number of samples to collect for each alternative system.
        alpha (float): Significance level, controlling the probability of incorrect selection.
        delta (float): Indifference zone parameter, representing the minimum difference between means that is
                       considered significant.

    Returns:
        The best alternative system from the provided list, with updated sample count and simulation time.
    """
    n_alt = len(ALT)  # Number of alternative systems
    h2 = (n0 - 1) * ((2 * alpha / (n_alt - 1)) ** (-2 / (n0 - 1)) - 1)  # Calculate the h2 parameter for KN procedure
    index = list(range(n_alt))  # Index list for tracking active alternatives
    sum_sample = []  # List to store the sum of initial samples for each alternative

    # Collect initial samples for each alternative and compute their sum
    sample = [[ALT[i].run_system() for _ in range(n0)] for i in range(n_alt)]
    for i in range(n_alt):
        sum_sample.append(sum(sample[i]))

    # Initialize the variance matrix s2
    s2 = [[0] * n_alt for _ in range(n_alt)]
    for i in range(1, n_alt):
        for j in range(i):
            # Calculate the sample variance between alternatives i and j
            s2[i][j] = (sum((sample[i][k] - sample[j][k] - (sum_sample[i] - sum_sample[j]) / n0) ** 2 for k in range(n0))
                        / (n0 - 1))
            s2[j][i] = s2[i][j]  # Symmetric matrix

    t = n0  # Current sample size
    num_sample = 0  # Total number of samples used in the procedure
    simulate_time = 0  # Total simulation time

    # Main loop to eliminate inferior alternatives until only one remains
    while len(index) > 1:
        t = max(t, 1)  # Ensure the sample size t is at least 1
        count = [0] * len(index)  # List to track the number of eliminations for each alternative

        # Compare each pair of alternatives
        for i in range(1, len(index)):
            for j in range(i):
                N = math.floor(s2[index[i]][index[j]] * h2 / (delta ** 2))  # Calculate the threshold sample size
                if t > N:
                    # Eliminate the inferior alternative based on the sum of samples
                    if sum_sample[index[i]] > sum_sample[index[j]]:
                        count[j] += 1  # Mark alternative j for elimination
                    else:
                        count[i] += 1  # Mark alternative i for elimination
                else:
                    # Check if alternatives can be eliminated based on confidence intervals
                    if sum_sample[index[i]] - sum_sample[index[j]] < -max(
                            h2 * s2[index[i]][index[j]] / (2 * delta) - delta * t / 2, 0):
                        count[i] += 1  # Mark alternative i for elimination
                    elif sum_sample[index[j]] - sum_sample[index[i]] < -max(
                            h2 * s2[index[i]][index[j]] / (2 * delta) - delta * t / 2, 0):
                        count[j] += 1  # Mark alternative j for elimination

        # Eliminate marked alternatives and update sample count and simulation time
        for i in reversed(range(len(count))):
            if count[i] != 0:
                num_sample += ALT[index[i]].get_num() + ALT[index[i]].get_N()  # Update total sample count
                simulate_time += ALT[index[i]].get_simulation_time()  # Update total simulation time
                del index[i]  # Remove the eliminated alternative from the index list

        # Break if only one alternative remains
        if len(index) == 1:
            break

        # Collect an additional sample for each remaining alternative
        for i in index:
            sum_sample[i] += ALT[i].run_system()
        t += 1  # Increment the sample size

    # Update the best alternative with the total sample count and simulation time
    ALT[index[0]].set_N(ALT[index[0]].get_N() + num_sample)
    ALT[index[0]].set_simulation_time(ALT[index[0]].get_simulation_time() + simulate_time)

    return ALT[index[0]]  # Return the best alternative


class KTPlus:
    """
    A class to perform the KTPlus selection procedure, which identifies the best alternative system from a list
    using a multi-round approach combined with the KN (Kim and Nelson) selection method.
    """

    def __init__(self, ALT, parm):
        """
        Initializes the KTPlus object with a list of alternative systems and the selection procedure parameters.

        Args:
            ALT (list): List of alternative systems to be compared.
            parm (list): Parameters for the KTPlus selection procedure, including:
                - alpha (float): Significance level.
                - delta (float): Indifference zone parameter.
                - n0 (int): Initial number of samples for each alternative.
                - g (int): Group size for grouping alternatives.
                - m (int): Total number of alternatives.
        """
        self.best_alternative = None  # Variable to store the best alternative after selection
        self.ALT = ALT  # List of alternative systems
        self.parm = parm  # Parameters for the selection procedure

    def get_best_alt(self):
        """
        Returns:
            The best alternative system identified by the KTPlus selection procedure.
        """
        return self.best_alternative

    def KTSelect(self):
        """
        Executes the KTPlus selection procedure to identify the best alternative. The procedure involves multiple
        rounds of selection, where alternatives are grouped and compared using the KN method. The best alternative
        from each group is carried forward to the next round until only one alternative remains.

        Procedure:
        1. Sorts the alternatives by their ID.
        2. Computes the number of rounds needed based on the number of alternatives and group size.
        3. In each round, adjusts the significance level and groups alternatives for comparison.
        4. Uses the KN selection method to identify the best alternative within each group.
        5. Carries forward the best alternatives to the next round.
        6. In the final round, calculates the required sample size using the Rinott constant and collects additional
           samples if necessary to estimate the mean of the best alternative.
        """
        # Step 1: Sort alternatives by their ID
        self.ALT.sort(key=lambda x: x.get_id())

        # Unpack parameters
        alpha = self.parm[0]
        delta = self.parm[1]
        n0 = int(self.parm[2])
        g = int(self.parm[3])
        m = int(self.parm[4])
        n_alt = len(self.ALT)

        # Step 2: Calculate the number of rounds needed
        R = math.ceil(math.log(n_alt) / math.log(g))

        # Step 3: Perform selection in multiple rounds
        for i in range(R):
            Ir = []  # List to store intermediate results of the current round
            r = i + 1
            alpha_r = alpha / (2 ** r)  # Adjust alpha for the current round

            # Step 4: Group alternatives and apply the KN selection method
            for j in range(math.ceil(len(self.ALT) / g)):
                group = []  # List to store a group of alternatives
                for k in range(len(self.ALT)):
                    if k % math.ceil(len(self.ALT) / g) == j:
                        group.append(self.ALT[k])
                SAM = KN(group, n0, alpha_r, delta)  # Apply the KN method to the group
                Ir.append(SAM)  # Store the best alternative of the group

            self.ALT = Ir  # Update the list of alternatives for the next round

        # Step 5: Final round - Calculate Rinott constant and estimate mean
        rm = R + 1
        alpha_rm = alpha / (2 ** rm)  # Adjust alpha for the final round
        h = rinott(m, 1 - alpha_rm, n0 - 1)  # Calculate the Rinott constant

        # Initialize variables for mean and variance calculation
        mean = 0
        si2 = 0

        # Collect initial samples and calculate the sample variance
        for i in range(n0):
            sa = self.ALT[0].run_system()  # Run the system to get a sample
            si2 += sa * sa  # Accumulate the sum of squares
            mean += sa / n0  # Calculate the mean

        s2 = (si2 - n0 * mean * mean) / (n0 - 1)  # Calculate the sample variance
        mean = mean * n0
        constant = math.ceil((h / delta) ** 2 * s2)  # Calculate the required number of samples

        # Step 6: Collect additional samples if needed
        for i in range(max(0, constant - n0)):
            add_sample = self.ALT[0].run_system()  # Run the system to get an additional sample
            mean += add_sample  # Update the mean

        # Final mean calculation
        mean = mean / max(n0, constant)
        self.ALT[0].set_mean(mean)  # Set the mean for the best alternative
        self.ALT[0].set_N(self.ALT[0].get_num() + self.ALT[0].get_N())  # Update the total sample count

        # Step 7: Set the best alternative
        self.best_alternative = self.ALT[0]  # Store the best alternative

