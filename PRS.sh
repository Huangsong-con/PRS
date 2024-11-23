#!/bin/bash

# Get the current script's directory (sh file location)
script_dir=$(pwd)

# Get the real IP address of the Linux system
real_ip=$(hostname -I | awk '{print $1}')

# Check if a 'SparkPRS' directory exists in the current directory
if [ -d "$script_dir/SparkPRS" ]; then
    # Set SPARK_HOME to SparkPRS directory in the current directory
    export SPARK_HOME="$script_dir/SparkPRS"
    export PATH=$SPARK_HOME/bin:$PATH
    echo "SPARK_HOME is set to $SPARK_HOME"
    echo "PATH is updated."
else
    echo "No 'SparkPRS' directory found in the current directory. Please ensure Spark is installed."
    exit 1
fi

# First, stop any running Spark processes
if [ -x "$SPARK_HOME/sbin/stop-all.sh" ]; then
    echo "Stopping all running Spark services..."
    "$SPARK_HOME/sbin/stop-all.sh"
else
    echo "Cannot find or execute $SPARK_HOME/sbin/stop-all.sh. Make sure Spark is correctly installed."
    exit 1
fi

# Clean the $SPARK_HOME/work directory but keep the 'work' folder itself
if [ -d "$SPARK_HOME/work" ]; then
    echo "Cleaning up the $SPARK_HOME/work directory..."
    rm -rf $SPARK_HOME/work/*
    echo "$SPARK_HOME/work is cleaned but the directory itself is retained."
else
    echo "$SPARK_HOME/work directory does not exist. Creating it now..."
    mkdir -p "$SPARK_HOME/work"
fi

# Check if JAVA_HOME is already set
if [ -z "$JAVA_HOME" ]; then
    echo "JAVA_HOME is not set."
    
    # Check if the default Java installation path exists
    if [ -d "/usr/lib/jvm/java-8-openjdk-amd64" ]; then
        # Set JAVA_HOME to the default installation path
        export JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64"
        export PATH=$JAVA_HOME/bin:$PATH
        echo "JAVA_HOME is set to default location: /usr/lib/jvm/java-8-openjdk-amd64"
        echo "PATH is updated."
    else
        # If Java is not installed, prompt the user to install it
        echo "Java8 is not installed. Please install Java8 before running this script."
        echo "You can install it using the following command:"
        echo "sudo apt-get install openjdk-8-jdk"
        exit 1
    fi
else
    echo "JAVA_HOME is already set to $JAVA_HOME"
    
    # Further check if Java in JAVA_HOME is version 8
    java_version=$("$JAVA_HOME/bin/java" -version 2>&1 | grep -i "version" | awk -F '"' '{print $2}')
    if [[ $java_version == 1.8* || $java_version == 8* ]]; then
        echo "Java version in JAVA_HOME is $java_version, which is compatible."
    else
        echo "Java version in JAVA_HOME is $java_version, which is not Java 8."
        echo "Please ensure JAVA_HOME points to a Java 8 installation."
        exit 1
    fi
fi


# If SPARK_HOME is set, proceed to create spark-env.sh
if [ -n "$SPARK_HOME" ]; then
    # Navigate to the conf directory under SPARK_HOME
    if [ ! -d "$SPARK_HOME/conf" ]; then
        echo "The conf directory does not exist under $SPARK_HOME"
        exit 1
    fi
    
    cd "$SPARK_HOME/conf"
    
    # Get total physical RAM in MB
    total_mem=$(awk '/MemTotal/ {print int($2 / 1024)}' /proc/meminfo)

    # Get the number of CPU cores
    cores=$(nproc)

    # Calculate SPARK_WORKER_MEMORY in MB
    SPARK_WORKER_MEMORY=${total_mem}m

    # Calculate SPARK_EXECUTOR_MEMORY as SPARK_WORKER_MEMORY / cores
    SPARK_EXECUTOR_MEMORY=$(($total_mem / $cores))m

    # Create or overwrite the spark-env.sh file with the new content, including JAVA_HOME and real IP
    echo "Creating spark-env.sh with the system's memory, CPU information, and real IP address..."

    cat <<EOL > spark-env.sh
export JAVA_HOME=$JAVA_HOME
export SPARK_MASTER_HOST=$real_ip
export SPARK_MASTER_PORT=7077
export SPARK_MASTER_WEBUI_PORT=8080
export SPARK_EXECUTOR_MEMORY=$SPARK_EXECUTOR_MEMORY
export SPARK_EXECUTOR_CORES=1
export SPARK_WORKER_CORES=$cores
export SPARK_WORKER_MEMORY=$SPARK_WORKER_MEMORY
EOL

    # echo "spark-env.sh created successfully with the following content:"
    # cat spark-env.sh

else
    echo "SPARK_HOME is not set. Cannot create spark-env.sh."
fi

# Prompt the user to choose between Java and Python
while true; do
    echo "========================================="
    echo "Please choose the version of PRS to use:"
    echo "1. Java version of PRS"
    echo "2. Python version of PRS"
    echo "========================================="

    read -p "Enter 1 for Java or 2 for Python: " choice

    if [ "$choice" -eq 2 ]; then
        echo "You chose Python."

        # Check if PRS.zip and alternatives.txt exist
        if [ ! -f "$script_dir/PRS.zip" ]; then
            echo "PRS.zip not found in $script_dir. Please add it and try again."
            exit 1
        fi

        if [ ! -f "$script_dir/alternatives.txt" ]; then
            echo "alternatives.txt not found in $script_dir. Please add it and try again."
            exit 1
        fi

        # Check if PYTHON_HOME is already set or if /usr/bin/python3.10 exists
        if [ -n "$PYTHON_HOME" ]; then
            echo "Using existing Python path: $PYTHON_HOME"
            # Check if Python executable exists and verify version
            if [ -x "$PYTHON_HOME/bin/python" ]; then
                python_version=$("$PYTHON_HOME/bin/python" --version 2>&1 | awk '{print $2}')
                if [[ $python_version == 3.10* ]]; then
                    echo "Python version at PYTHON_HOME is Python 3.10: $python_version"
                else
                    echo "ERROR: Python version at PYTHON_HOME is not Python 3.10. Current version: $python_version"
                    echo "Please set PYTHON_HOME to a Python 3.10 installation."
                    exit 1
                fi
            else
                echo "ERROR: No executable Python binary found in $PYTHON_HOME/bin."
                exit 1
            fi
        elif [ -x "/usr/bin/python3.10" ]; then
            # If Python 3.10 is found in the expected path, get the real path using readlink
            PYTHON_HOME=$(readlink -f /usr/bin/python3.10)
            echo "Python 3.10 is found at: $PYTHON_HOME"
        else
            # If Python 3.10 does not exist, prompt the user to install it
            echo "Python 3.10 is not installed at /usr/bin/python3.10. Please install it using the following command:"
            echo "sudo apt-get install python3.10"
            exit 1
        fi

        # Ensure PRS directory exists
        PRS_DIR="$SPARK_HOME/work/PRS"
        if [ ! -d "$PRS_DIR" ]; then
            echo "Creating PRS directory at $PRS_DIR"
            mkdir "$PRS_DIR"
        fi

        # Unzip PRS.zip into the PRS directory
        echo "Extracting PRS.zip to $PRS_DIR"
        unzip -o "$script_dir/PRS.zip" -d "$PRS_DIR"
        
        # Run Spark start-all.sh after selecting Java
        "$SPARK_HOME/sbin/start-all.sh"

        # Run spark-submit with PYSPARK_PYTHON set to PYTHON_PATH
        if [ -f "$PRS_DIR/UserInterface/GUI.py" ]; then
            "$SPARK_HOME/bin/spark-submit" --master spark://$real_ip:7077 --py-files "$script_dir/PRS.zip" --conf spark.pyspark.python=$PYTHON_HOME "$PRS_DIR/UserInterface/GUI.py" "$script_dir/alternatives.txt"
        else
            echo "GUI.py not found in the PRS directory."
            exit 1
        fi
        break

    elif [ "$choice" -eq 1 ]; then
        echo "You chose Java."

        # Check if PRS.jar and alternatives.txt exist
        if [ ! -f "$script_dir/PRS.jar" ]; then
            echo "PRS.jar not found in $script_dir. Please add it and try again."
            exit 1
        fi

        if [ ! -f "$script_dir/alternatives.txt" ]; then
            echo "alternatives.txt not found in $script_dir. Please add it and try again."
            exit 1
        fi

        # Run Spark start-all.sh after selecting Java
        "$SPARK_HOME/sbin/start-all.sh"
        
        # Java-specific spark-submit command can be added here if needed
        echo "Java setup done. You can now submit your Java Spark job."
        "$SPARK_HOME/bin/spark-submit" --master spark://$real_ip:7077 --class "UserInterface.GUI" "$script_dir/PRS.jar" "$script_dir/alternatives.txt"
        break

    else
        echo "Invalid choice. Please enter 1 or 2."
    fi
done

