import os
import sys
from tkinter import ttk, messagebox
from AlgoExec_M.GSP import run_gsp_process, run_gsp_process_with_stage0
from AlgoExec_M.FBKT import run_fbkt_process
from AlgoExec_M.KT import run_kt_process
import tkinter as tk


# Class to create and display tooltips for parameter descriptions
class ToolTip:
    def __init__(self, widget, text):
        """
        Initialize the ToolTip object.

        Args:
            widget: The Tkinter widget to attach the tooltip to.
            text: The text to display in the tooltip.
        """
        self.widget = widget
        self.text = text
        self.tooltip_window = None
        widget.bind("<Enter>", self.show_tooltip)
        widget.bind("<Leave>", self.hide_tooltip)

    def show_tooltip(self, event):
        """
        Display the tooltip when the mouse enters the widget.
        """
        if self.tooltip_window or not self.text:
            return
        x, y, _, _ = self.widget.bbox("insert")
        x += self.widget.winfo_rootx() + 20
        y += self.widget.winfo_rooty() + 20
        self.tooltip_window = tw = tk.Toplevel(self.widget)
        tw.wm_overrideredirect(True)
        tw.wm_geometry(f"+{x}+{y}")
        label = tk.Label(tw, text=self.text, background="yellow", relief="solid", borderwidth=1, wraplength=200)
        label.pack(ipadx=1)

    def hide_tooltip(self, event):
        """
        Hide the tooltip when the mouse leaves the widget.
        """
        if self.tooltip_window:
            self.tooltip_window.destroy()
        self.tooltip_window = None


# Function to validate parameters for the GSP algorithm
def validate_gsp_args(params):
    """
    Validate the parameters for the GSP algorithm.

    Args:
        params: List of parameters to validate.

    Returns:
        bool: True if parameters are valid, False otherwise.
    """
    try:
        if int(params[1]) <= 0 or int(params[2]) <= 0 or int(params[3]) <= 0:
            return False
        if not (0 < float(params[4]) < 1) or not (0 < float(params[5]) < 1):
            return False
        if float(params[6]) <= 0 or int(params[7]) <= 0 or int(params[8]) <= 0:
            return False
        return True
    except ValueError:
        return False


# Function to validate parameters for the KT algorithm
def validate_kt_args(params):
    """
    Validate the parameters for the KT algorithm.

    Args:
        params: List of parameters to validate.

    Returns:
        bool: True if parameters are valid, False otherwise.
    """
    try:
        if not (0 < float(params[0]) < 1) or float(params[1]) <= 0:
            return False
        if int(params[2]) <= 0 or int(params[3]) <= 0 or int(params[4]) <= 0 or int(params[5]) <= 0 or int(
                params[6]) <= 0:
            return False
        return True
    except ValueError:
        return False


# Function to validate parameters for the FBKT algorithm
def validate_fbkt_args(params):
    """
    Validate the parameters for the FBKT algorithm.

    Args:
        params: List of parameters to validate.

    Returns:
        bool: True if parameters are valid, False otherwise.
    """
    try:
        if float(params[0]) <= 0 or int(params[1]) <= 0 or int(params[2]) <= 0 or int(params[3]) <= 0 or int(
                params[4]) <= 0 or int(params[5]) <= 0:
            return False
        return True
    except ValueError:
        return False


# Function to run the selected algorithm and output results to a file
def run_algorithm(algorithm_choice1, params, parameters_path):
    """
    Run the specified algorithm and output results to a file.

    Args:
        algorithm_choice1: The algorithm to run (GSP, KT, or FBKT).
        params: List of parameters for the algorithm.
        parameters_path: Path to the input parameters file.
    """
    try:
        if not os.path.exists(parameters_path):
            raise FileNotFoundError(f"alternatives.txt file not found at: {parameters_path}")

        # Prepare the output file path
        output_directory = os.path.dirname(parameters_path)
        output_file_path = os.path.join(output_directory, "output.txt")

        with open(output_file_path, 'w') as output_file:
            # Run the GSP algorithm
            if algorithm_choice1 == "GSP":
                if not validate_gsp_args(params):
                    raise ValueError("Invalid GSP parameters.")
                # Extract and convert parameters
                n0 = int(params[0]) if params[0] else 0
                n1, rMax, beta = int(params[1]), int(params[2]), int(params[3])
                alpha1, alpha2, delta = float(params[4]), float(params[5]), float(params[6])
                nCores, nAlternative = int(params[7]), int(params[8])
                seed = int(params[9]) if len(params) >= 10 and params[9] else 1234
                repeat = int(params[10]) if len(params) == 11 and params[10] else 1

                # Write input parameters to the output file
                output_file.write(f"Chosen Procedure: The GSP Procedure\n\n")
                output_file.write(f"Input Parameters:\n")
                output_file.write(f"n0 = {n0}, n1 = {n1}, rMax = {rMax}, beta = {beta}, ")
                output_file.write(f"alpha1 = {alpha1}, alpha2 = {alpha2}, delta = {delta}, ")
                output_file.write(
                    f"# of processors = {nCores}, # of alternatives = {nAlternative}, seed = {seed},"
                    f" and repeat = {repeat}.\n\n")
                output_file.write("BestID: The index of the selected best alternative.\n")
                output_file.write("WCT: Wall-Clock Time.\n")
                output_file.write("TST: Total Simulation Time.\n\n")
                output_file.write(f"{'Replication':<15}{'BestID':<12}{'WCT (Seconds)':<18}{'TST (Seconds)':<18}\n")

                # Run the appropriate GSP process
                if n0 == 0:
                    results = run_gsp_process(n1, rMax, beta, alpha1, alpha2, delta, nCores, parameters_path,
                                              nAlternative, seed, repeat)
                else:
                    results = run_gsp_process_with_stage0(n0, n1, rMax, beta, alpha1, alpha2, delta, nCores,
                                                          parameters_path, nAlternative, seed, repeat)

                # Write results to the file
                for replication, best_id, wall_clock_time, total_simulate_time in results:
                    output_file.write(
                        f"{replication:<15}{best_id:<12}{wall_clock_time:<18.2f}{total_simulate_time:<18.2f}\n")

            # Run the KT algorithm
            elif algorithm_choice1 == "KT":
                # Extract and convert parameters
                alpha, delta = float(params[0]), float(params[1])
                n0, g, nCores, nAlternative = int(params[2]), int(params[3]), int(params[4]), int(params[5])
                seed = int(params[6]) if len(params) >= 7 and params[6] else 1234
                repeat = int(params[7]) if len(params) == 8 and params[7] else 1

                # Write input parameters to the output file
                output_file.write(f"Chosen Procedure: The KT Procedure\n\n")
                output_file.write(f"Input Parameters:\n")
                output_file.write(f"alpha = {alpha}, delta = {delta}, n0 = {n0}, g = {g}, ")
                output_file.write(
                    f"# of processors = {nCores}, # of alternatives = {nAlternative}, seed = {seed}, "
                    f"and repeat = {repeat}.\n\n")
                output_file.write("BestID: The index of the selected best alternative.\n")
                output_file.write("WCT: Wall-Clock Time.\n")
                output_file.write("TST: Total Simulation Time.\n\n")
                output_file.write(f"{'Replication':<15}{'BestID':<12}{'WCT (Seconds)':<18}{'TST (Seconds)':<18}\n")

                # Run KT process and write results to the file
                results = run_kt_process(alpha, delta, n0, g, nCores, parameters_path, seed, repeat)
                for replication, best_id, wall_clock_time, total_simulate_time in results:
                    output_file.write(
                        f"{replication:<15}{best_id:<12}{wall_clock_time:<18.2f}{total_simulate_time:<18.2f}\n")

            # Run the FBKT algorithm
            elif algorithm_choice1 == "FBKT":
                # Extract and convert parameters
                N, n0, phi, nCores, nAlternative = int(params[0]), int(params[1]), int(params[2]), int(
                    params[3]), int(params[4])
                seed = int(params[5]) if len(params) >= 6 and params[5] else 1234
                repeat = int(params[6]) if len(params) == 7 and params[6] else 1

                # Write input parameters to the output file
                output_file.write(f"Chosen Procedure: The FBKT Procedure\n\n")
                output_file.write(f"Input Parameters:\n")
                output_file.write(f"N = {int(N)}, n0 = {n0}, phi = {phi}, # of processors = {nCores}, ")
                output_file.write(f"# of alternatives = {nAlternative}, seed = {seed}, and repeat = {repeat}.\n\n")
                output_file.write("BestID: The index of the selected best alternative.\n")
                output_file.write("WCT: Wall-Clock Time.\n")
                output_file.write("TST: Total Simulation Time.\n\n")
                output_file.write(f"{'Replication':<15}{'BestID':<12}{'WCT (Seconds)':<18}{'TST (Seconds)':<18}\n")

                # Run FBKT process and write results to the file
                results = run_fbkt_process(N, n0, phi, nCores, parameters_path, seed, repeat)
                for replication, best_id, wall_clock_time, total_simulate_time in results:
                    output_file.write(
                        f"{replication:<15}{best_id:<12}{wall_clock_time:<18.2f}{total_simulate_time:<18.2f}\n")

            print(f"Results have been written to {output_file_path}")

    except Exception as ex:
        messagebox.showerror("Error", str(ex))


# Function to dynamically display parameters and the Run button based on the selected algorithm
def display_parameters(selected_algorithm):
    """
    Display parameter input fields and the Run button for the selected algorithm.

    Args:
        selected_algorithm: The algorithm selected (GSP, KT, or FBKT).
    """
    for widget in parameter_frame.winfo_children():
        widget.destroy()

    param_labels = []
    if selected_algorithm == "GSP":
        param_labels = [
            ("n0 (default=0)", "Sample size of Stage 0."),
            ("n1", "Sample size of Stage 1."),
            ("rMax", "The maximum number of rounds in Stage 2."),
            ("beta", "Average number of simulation samples taken from an alternative in each round in Stage 2."),
            ("alpha1",
             "The split of the tolerable probability of incorrect selection alpha such that alpha = alpha1 + alpha2."),
            ("alpha2",
             "The split of the tolerable probability of incorrect selection alpha such that alpha = alpha1 + alpha2."),
            ("delta", "Indifference zone (IZ) parameter."),
            ("# of processors", "Number of processors used to run the procedure."),
            ("# of alternatives", "Number of alternatives for the problem."),
            ("Seed (default=1234)", "Seed used to generate random numbers."),
            ("Repeat (default=1)", "The number of times the problem is repeatedly solved.")
        ]
    elif selected_algorithm == "KT":
        param_labels = [
            ("alpha", "Tolerable probability of incorrect selection."),
            ("delta", "Indifference zone (IZ) parameter."),
            ("n0", "First-stage sample size when using the KN procedure."),
            ("g", "Number of alternatives in a group."),
            ("# of processors", "Number of processors used to run the procedure."),
            ("# of alternatives", "Number of alternatives for the problem."),
            ("Seed (default=1234)", "Seed used to generate random numbers."),
            ("Repeat (default=1)", "The number of times the problem is repeatedly solved.")
        ]
    elif selected_algorithm == "FBKT":
        param_labels = [
            ("N", "Total sample budget."),
            ("n0", "Sample size needed at the initial stage for seeding."),
            ("phi", "A positive integer that determines budget allocation."),
            ("# of processors", "Number of processors used to run the procedure."),
            ("# of alternatives", "Number of alternatives for the problem."),
            ("Seed (default=1234)", "Seed used to generate random numbers."),
            ("Repeat (default=1)", "The number of times the problem is repeatedly solved.")
        ]

    parameter_entries.clear()
    for idx, (label, tooltip) in enumerate(param_labels):
        lbl = ttk.Label(parameter_frame, text=label)
        lbl.grid(row=idx, column=0, padx=5, pady=5)
        ToolTip(lbl, tooltip)  # Attach the tooltip with parameter description
        entry = ttk.Entry(parameter_frame)
        entry.grid(row=idx, column=1, padx=5, pady=5)
        parameter_entries.append(entry)

    # Add the Run button
    run_button = ttk.Button(parameter_frame, text="Run", command=lambda: on_run(selected_algorithm))
    run_button.grid(row=len(param_labels) + 2, column=0, columnspan=2, pady=20)


# Function to handle running the selected algorithm
def on_run(selected_algorithm):
    """
    Run the selected algorithm with the provided parameters.

    Args:
        selected_algorithm: The algorithm selected (GSP, KT, or FBKT).
    """
    params = [entry.get() for entry in parameter_entries]
    parameters_path = sys.argv[1]  # Get the parameters path from the command-line argument
    run_algorithm(selected_algorithm, params, parameters_path)


# Function to handle algorithm selection and display parameters
def on_algorithm_selected(event):
    """
    Handle the event when an algorithm is selected from the dropdown.

    Args:
        event: The event object.
    """
    selected_algorithm = algorithm_choice.get()
    if selected_algorithm:
        display_parameters(selected_algorithm)


# Set up the main window
root = tk.Tk()
root.title("Procedure Selector")
root.geometry("500x600")

# Create a dropdown for selecting the algorithm
algorithm_choice = tk.StringVar()
ttk.Label(root, text="Select Procedure:").pack(pady=10)
algorithm_dropdown = ttk.Combobox(root, textvariable=algorithm_choice, values=["GSP", "KT", "FBKT"], state="readonly")
algorithm_dropdown.pack(pady=10)
algorithm_dropdown.bind("<<ComboboxSelected>>", on_algorithm_selected)

# Create a frame for parameter input
parameter_frame = ttk.Frame(root)
parameter_frame.pack(pady=10)

# List to store parameter entry widgets
parameter_entries = []

# Run the main event loop
root.mainloop()
