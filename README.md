# PRS：A Software Package for Parallel Ranking and Selection Procedures

PRS includes three popular parallel RS procedures, i.e., the good selection procedure (**GSP**) proposed by [ Ni et al. (2017)](https://doi.org/10.1287/opre.2016.1577), the knockout-tournament (**KT**) procedure proposed by [ Zhong and Hong (2022)](https://doi.org/10.1287/opre.2020.2065), and the fixed-budget knockout-tournament (**FBKT**) procedure proposed by [ Hong et al. (2022)](https://doi.org/10.1287/ijoc.2022.1221).


# File and Directory Overview

This package includes the source code for both the Java and Python versions of PRS, along with a user manual, an executable file, named winutils.exe, and two script files: PRS.cmd and PRS.sh.
- **Code for Java version of PRS**: This package includes the source code for the Java version of PRS.
- **Code for Python version of PRS**: This package includes the source code for the Java version of PRS.
- **PRS.cmd**: This script file is used to launch PRS on Windows systems.
- **winutils.exe**: This file ensures compatibility for running Apache Spark on Windows systems.
- **PRS.sh**: This script file is used to launch PRS on Debian-based Linux systems.
- **PRS User Manual.pdf**: The user manual offers detailed, step-by-step instructions for applying the software to solve real-world problems.

# Software Version

```plaintext
PRS Version: 1.0
Supported Platforms: Windows, Debian-based Linux
Java Version Required: 1.8
Python Version Required: 3.10
Spark Version Required: 3.4.0
```

# References
Ni EC, Ciocan DF, Henderson SG, Hunter SR (2017) Efficient ranking and selection in parallel computing environments. *Operations Research, 65*(3), 821–836.  
Zhong Y, Hong LJ (2022) Knockout-tournament procedures for large-scale ranking and selection in parallel computing environments. *Operations Research, 70*(1), 432–453.  
Hong LJ, Jiang G, Zhong Y (2022) Solving large-scale fixed-budget ranking and selection problems. *INFORMS Journal on Computing, 34*(6), 2930–2949.  
