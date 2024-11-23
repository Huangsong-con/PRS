@echo off
setlocal enabledelayedexpansion

REM Search for the SparkPRS directory in the current directory
if exist "%cd%\SparkPRS" (
    set "SPARK_HOME=%cd%\SparkPRS"
) else (
    echo ERROR: SparkPRS directory not found in the current directory.
    exit /b 1
)

REM Clean up bin and log directories, delete contents of the work directory but retain the folder
del /q "!SPARK_HOME!\work\*.*" 2>nul
for /d %%i in ("!SPARK_HOME!\work\*") do rd /s /q "%%i" 2>nul

REM Check if JAVA_HOME is set
if not defined JAVA_HOME (
    if exist "C:\Program Files\Java\jdk-1.8" (
        echo JAVA_HOME not set, using default Java 8 path.
        set "JAVA_HOME=C:\Program Files\Java\jdk-1.8"
    ) else (
        echo ERROR: JAVA_HOME is not set and default Java 8 path not found.
        exit /b 1
    )
) else (
    echo JAVA_HOME is set to %JAVA_HOME%
    REM Check if JAVA_HOME points to Java 8
    if exist "%JAVA_HOME%\bin\java.exe" (
        REM Capture Java version into a temporary file
        "%JAVA_HOME%\bin\java.exe" -version > "%temp%\java_version.txt" 2>&1
        findstr "1.8" "%temp%\java_version.txt" >nul
        if errorlevel 1 (
            echo ERROR: JAVA_HOME does not point to Java 8. Please set it correctly.
            del "%temp%\java_version.txt"
            exit /b 1
        ) else (
            echo JAVA_HOME points to Java 8.
            del "%temp%\java_version.txt"
        )
    ) else (
        echo ERROR: No executable Java found in %JAVA_HOME%\bin.
        exit /b 1
    )
)

REM Add JAVA_HOME to PATH
set "PATH=!JAVA_HOME!\bin;%PATH%"

REM Set the current directory as HADOOP_HOME and add it to PATH
set "HADOOP_HOME=!SPARK_HOME!\work"
set "PATH=!HADOOP_HOME!\bin;%PATH%"
echo HADOOP_HOME is set to !HADOOP_HOME! and added to PATH

REM Check if the bin directory exists, if not, create it
if not exist "!HADOOP_HOME!\bin" (
    echo bin directory not found, creating it...
    mkdir "!HADOOP_HOME!\bin"
) else (
    echo bin directory already exists.
)

REM Check if winutils.exe exists in the current directory
if exist "%~dp0winutils.exe" (
    echo winutils.exe found, copying it to the bin directory...
    copy "%~dp0winutils.exe" "!HADOOP_HOME!\bin\winutils.exe"
) else (
    echo ERROR: winutils.exe not found in the current directory. Please place winutils.exe in this directory.
    exit /b 1
)

REM Automatically get the number of available CPU cores
set WORKER_CORES=%NUMBER_OF_PROCESSORS%

REM Get total RAM in MB using PowerShell
for /f "tokens=*" %%A in ('powershell -command "Get-CimInstance -ClassName Win32_ComputerSystem | ForEach-Object { [math]::floor($_.TotalPhysicalMemory / 1MB) }"') do set WORKER_MEMORY=%%A

REM Calculate EXECUTOR_MEMORY as WORKER_MEMORY divided by the number of processors (cores)
set /a EXECUTOR_MEMORY=%WORKER_MEMORY% / %WORKER_CORES%

echo Total Physical Memory: %WORKER_MEMORY% MB
echo Executor Memory: %EXECUTOR_MEMORY% MB

REM Get the local IP address from ipconfig output (the first IPv4 address)
for /f "tokens=2 delims=:" %%a in ('ipconfig ^| findstr /i "IPv4"') do for /f "tokens=1 delims= " %%b in ("%%a") do set SPARK_MASTER_HOST=%%b

REM Remove any leading spaces in the IP address
set "SPARK_MASTER_HOST=%SPARK_MASTER_HOST: =%"

cd /d "!SPARK_HOME!"
if not exist conf mkdir conf
if not exist sbin mkdir sbin

cd "%SPARK_HOME%\conf"

REM Write values to spark-env.cmd with delayed expansion
(
    echo set JAVA_HOME=!JAVA_HOME!
    echo set SPARK_MASTER_HOST=!SPARK_MASTER_HOST!
    echo set SPARK_MASTER_PORT=7077
    echo set SPARK_MASTER_WEBUI_PORT=8080
    echo set SPARK_EXECUTOR_MEMORY=!EXECUTOR_MEMORY!m
    echo set SPARK_EXECUTOR_CORES=1
    echo set SPARK_WORKER_CORES=!WORKER_CORES!
    echo set SPARK_WORKER_MEMORY=!WORKER_MEMORY!m
) > spark-env.cmd

cd "%SPARK_HOME%\sbin"

REM Create start-master.cmd
(
    echo @echo off
    echo setlocal enabledelayedexpansion
    echo REM Set SPARK_HOME explicitly if needed
    echo set "SPARK_HOME=!SPARK_HOME!"
    echo REM Start Spark master
    echo call "!SPARK_HOME!\bin\spark-class.cmd" org.apache.spark.deploy.master.Master
) > start-master.cmd

REM Create start-slaves.cmd
(
    echo @echo off
    echo setlocal enabledelayedexpansion
    echo set SPARK_HOME=!SPARK_HOME!
    echo set MASTER_URL=spark://!SPARK_MASTER_HOST!:7077
    echo call "!SPARK_HOME!\bin\spark-class.cmd" org.apache.spark.deploy.worker.Worker --webui-port 9090 spark://!SPARK_MASTER_HOST!:7077
    echo endlocal
) > start-slaves.cmd

REM Check if log directory exists, if not, create it
if not exist "!HADOOP_HOME!\log" (
    echo log directory not found, creating it...
    mkdir "!HADOOP_HOME!\log"
) else (
    echo log directory already exists.
)

REM Wait for 5 seconds before starting master
call :loading 5 "loading..."

REM Start Spark Master and redirect output to log file
echo Starting Spark Master...
start /b cmd /c "!SPARK_HOME!\sbin\start-master.cmd" > "!HADOOP_HOME!\log\master.log" 2>&1

REM Wait for 5 seconds before starting slaves
echo Waiting 5 seconds before starting Spark Workers...
call :loading 5 "Waiting for Spark Master to stabilize..."

REM Start Spark Workers and redirect output to log file
echo Starting Spark Workers...
start /b cmd /c "!SPARK_HOME!\sbin\start-slaves.cmd" > "!HADOOP_HOME!\log\slaves.log" 2>&1

REM Check for successful connection by looking for "Registered worker" in the logs
set "CONNECTION_ESTABLISHED=false"

REM :check_connection label definition with no indentation
:check_connection
findstr /i "Registered worker" "!HADOOP_HOME!\log\slaves.log" >nul
if not errorlevel 1 (
    set CONNECTION_ESTABLISHED=true
)

if "!CONNECTION_ESTABLISHED!"=="false" (
    echo Waiting for Spark Master and Workers to establish connection...
    call :loading 10 "Loading, please be patient"
    goto check_connection
)

echo Connection established between Spark Master and Workers.

REM Now provide option to choose between Java or Python version
:choose_version
echo =========================================
echo Please choose the version of PRS to use:
echo 1. Java version of PRS
echo 2. Python version of PRS
echo =========================================

set /p choice="Enter 1 for Java or 2 for Python: "

if "%choice%" == "1" (
    echo You chose the Java version.
    REM Check if PRS.jar and alternatives.txt exist
    if not exist "%~dp0PRS.jar" (
        echo ERROR: PRS.jar not found in the current directory!
        goto choose_version
    )

    if not exist "%~dp0alternatives.txt" (
        echo ERROR: alternatives.txt not found in the current directory!
        goto choose_version
    )
    cd /d "!SPARK_HOME!\bin"
    spark-submit --master spark://!SPARK_MASTER_HOST!:7077 --conf spark.executor.extraJavaOptions=-Dhadoop.home.dir=!HADOOP_HOME! --class UserInterface.GUI "%~dp0PRS.jar" "%~dp0alternatives.txt"
) else if "%choice%" == "2" (
    echo You chose the Python version.
    REM Check if PRS.zip and alternatives.txt exist
    if not exist "%~dp0PRS.zip" (
        echo ERROR: PRS.zip not found in the current directory!
        goto choose_version
    )
    if not exist "!HADOOP_HOME!\PRS" (
        mkdir "!HADOOP_HOME!\PRS"
        echo Created PRS directory.
    ) else (
        echo PRS directory already exists.
    )
    
    echo Extracting PRS.zip to !HADOOP_HOME!\PRS
    powershell -command "Expand-Archive -Path '%~dp0PRS.zip' -DestinationPath '!HADOOP_HOME!\PRS' -Force"
    
    REM Check if PYTHON_HOME exists, if not, use default Python 3.10 path
    if not defined PYTHON_HOME (
        if exist "C:\Users\%USERNAME%\AppData\Local\Programs\Python\Python310" (
            echo PYTHON_HOME not set, using default Python 3.10 path.
            set "PYTHON_HOME=C:\Users\%USERNAME%\AppData\Local\Programs\Python\Python310"
            echo PYTHON_HOME is set to !PYTHON_HOME!
        ) else (
            echo ERROR: PYTHON_HOME is not set and default Python 3.10 path not found.
            exit /b 1
        )
    ) else (
        echo PYTHON_HOME is set to !PYTHON_HOME!
        REM Verify if PYTHON_HOME points to Python 3.10
        if exist "!PYTHON_HOME!\python.exe" (
            for /f "tokens=2 delims=. " %%i in ('"!PYTHON_HOME!\python.exe" --version ^| find "3.10"') do (
                if "%%i"=="3" (
                    echo Python version is Python 3.10.
                ) else (
                    echo ERROR: Python version at PYTHON_HOME is not Python 3.10.
                    exit /b 1
                )
            )
        ) else (
            echo ERROR: No executable Python binary found in !PYTHON_HOME!\python.exe.
            exit /b 1
        )
    )

    REM Set PYSPARK_PYTHON to python.exe in PYTHON_HOME
    set "PYSPARK_PYTHON=!PYTHON_HOME!\python.exe"
    echo PYSPARK_PYTHON is set to !PYSPARK_PYTHON!
    
    cd /d "!SPARK_HOME!\bin"
    spark-submit --master spark://!SPARK_MASTER_HOST!:7077 --conf spark.executor.extraJavaOptions=-Dhadoop.home.dir=!HADOOP_HOME! --conf spark.pyspark.python=!PYSPARK_PYTHON! --py-files "%~dp0PRS.zip" "%HADOOP_HOME%\PRS\UserInterface\GUI.py" "%~dp0alternatives.txt"
) else (
    echo Invalid choice. Please enter 1 or 2.
    pause
    goto choose_version
)

REM :loading label and loop implementation
:loading
set count=%1
echo %2
:loading_loop
if %count% leq 0 goto :eof
timeout /t 1 /nobreak >nul
set /a count-=1
goto loading_loop

REM Terminate Spark master and worker processes on exit
echo Stopping Spark master and worker processes...

REM Find and kill start-master.cmd and start-slaves.cmd processes
for /f "tokens=2 delims= " %%i in ('tasklist /v ^| findstr /i "start-master.cmd start-slaves.cmd"') do (
    taskkill /pid %%i /f
)

REM Alternatively, kill all java.exe processes related to Spark if necessary
taskkill /f /im "java.exe" /t

pause
endlocal

