import math


def ZCDF(x):
    """
    Calculate the cumulative distribution function (CDF) of the standard normal distribution.

    Args:
        x (float): The input value.

    Returns:
        float: The CDF value of the standard normal distribution at x.
    """
    neg = 1 if x < 0 else 0  # Check if x is negative
    if neg == 1:
        x *= -1  # Take the absolute value of x if it is negative
    k = 1 / (1 + 0.2316419 * x)  # Calculate a constant k used in the approximation
    # Calculate a constant k used in the approximation
    y = ((((1.330274429 * k - 1.821255978) * k + 1.781477937) * k - 0.356563782) * k + 0.319381530) * k
    y = 1.0 - 0.3989422803 * math.exp(-0.5 * x * x) * y  # Adjust using the standard normal distribution formula
    return (1 - neg) * y + neg * (1 - y)  # Adjust for the sign of x


def CHIPDF(N, C, lngam):
    """
    Calculate the probability density function (PDF) of the chi-squared distribution.

    Args:
        N (int): Degrees of freedom.
        C (float): The chi-squared value.
        lngam (list[float]): List of precomputed logarithm of gamma values.

    Returns:
        float: The PDF value of the chi-squared distribution at C.
    """
    FLN2 = N / 2.0  # Calculate N/2
    TMP = -FLN2 * math.log(2) - lngam[N - 1] + (FLN2 - 1) * math.log(C) - C / 2.0  # Calculate the log-PDF
    return math.exp(TMP)  # Return the exponential of the log-PDF


def rinott(T, PSTAR, NU):
    """
    Calculate the Rinott constant using the bisection method and Gauss-Hermite quadrature.

    Args:
        T (int): Number of systems.
        PSTAR (float): Desired coverage probability (1 - alpha).
        NU (int): Stage-0 sample size minus 1.

    Returns:
        float: The computed Rinott constant.
    """
    # Initialize the logarithm of gamma values
    LNGAM = [0.5723649429] + [0.0] * 49
    NU = min(NU, 50)  # Ensure NU does not exceed 50
    WEX = [0.0] * 32  # Initialize the weighted exponential values
    # Gauss-Hermite quadrature weights and abscissas
    W = [0.10921834195238497114, 0.21044310793881323294, 0.23521322966984800539, 0.19590333597288104341,
         0.12998378628607176061, 0.70578623865717441560E-1, 0.31760912509175070306E-1, 0.11918214834838557057E-1,
         0.37388162946115247897E-2, 0.98080330661495513223E-3, 0.21486491880136418802E-3, 0.39203419679879472043E-4,
         0.59345416128686328784E-5, 0.74164045786675522191E-6, 0.76045678791207814811E-7, 0.63506022266258067424E-8,
         0.42813829710409288788E-9, 0.23058994918913360793E-10, 0.97993792887270940633E-12, 0.32378016577292664623E-13,
         0.81718234434207194332E-15, 0.15421338333938233722E-16, 0.21197922901636186120E-18, 0.20544296737880454267E-20,
         0.13469825866373951558E-22, 0.56612941303973593711E-25, 0.14185605454630369059E-27, 0.19133754944542243094E-30,
         0.11922487600982223565E-33, 0.26715112192401369860E-37, 0.13386169421062562827E-41, 0.45105361938989742322E-47]
    X = [0.44489365833267018419E-1, 0.23452610951961853745, 0.57688462930188642649, 1.0724487538178176330,
         1.7224087764446454411, 2.5283367064257948811, 3.4922132730219944896, 4.6164567697497673878,
         5.9039585041742439466, 7.3581267331862411132, 8.9829409242125961034, 10.783018632539972068,
         12.763697986742725115, 14.931139755522557320, 17.292454336715314789, 19.855860940336054740,
         22.630889013196774489, 25.628636022459247767, 28.862101816323474744, 32.346629153964737003,
         36.100494805751973804, 40.145719771539441536, 44.509207995754937976, 49.224394987308639177,
         54.333721333396907333, 59.892509162134018196, 65.975377287935052797, 72.687628090662708639,
         80.187446977913523067, 88.735340417892398689, 98.829542868283972559, 111.75139809793769521]

    # Compute weighted exponential values for Gauss-Hermite quadrature
    for I in range(1, 33):
        WEX[I - 1] = W[I - 1] * math.exp(X[I - 1])

    # Compute the logarithm of gamma values using Lanczos approximation
    for i in range(2, 26):
        LNGAM[2 * i - 2] = math.log(i - 1.5) + LNGAM[2 * i - 4]
        LNGAM[2 * i - 1] = math.log(i - 1.0) + LNGAM[2 * i - 3]

    # Initialize variables for the bisection method
    DUMMY = 1.0
    H = 4.0
    LOWERH = 0.0
    UPPERH = 20.0

    # Perform bisection method to find the Rinott constant
    for LOOPH in range(1, 51):
        ANS = 0.0
        for J in range(1, 33):
            TMP = 0.0
            for I in range(1, 33):
                # Calculate terms for the Gauss-Hermite quadrature
                TMP += WEX[I - 1] * ZCDF(H / math.sqrt(NU * (1 / X[I - 1] + 1 / X[J - 1]) / DUMMY)) \
                       * CHIPDF(NU, DUMMY * X[I - 1], LNGAM) * DUMMY
            TMP = TMP ** (T - 1)
            ANS += WEX[J - 1] * TMP * CHIPDF(NU, DUMMY * X[J - 1], LNGAM) * DUMMY
        # Check if the answer is within the desired tolerance
        if abs(ANS - PSTAR) <= 0.000001:
            return H  # Return H if convergence is achieved
        elif ANS > PSTAR:
            UPPERH = H  # Adjust the upper bound
            H = (LOWERH + UPPERH) / 2.0
        else:
            LOWERH = H  # Adjust the lower bound
            H = (LOWERH + UPPERH) / 2.0
    return H  # Return the final value of H after iterations
