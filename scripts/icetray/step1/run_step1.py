import argparse
import subprocess

def run_commands_parallel(commands, max_num=20):
    processes = []
    for command in commands:
        if len(processes) > max_num:
            processes[0].wait()
            processes = processes[1:]

        process = subprocess.Popen(command, shell=True)
        processes.append(process)

    # Wait for all processes to complete
    for process in processes:
        process.wait()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
                    
)
    


  num_processes = 5
  commands = [f"sleep {i}" for i in range(1, num_processes + 1)] # Example commands
  run_commands_parallel(commands)
  print("All processes finished")
