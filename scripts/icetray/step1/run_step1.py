import argparse
import subprocess
import concurrent.futures

def run_commands_parallel(commands, max_num=20):
    with concurrent.futures.ProcessPoolExecutor() as executor:

def run_command(


def generate_command(infiles, gcd):
    base_env = "s"
    base_icetray_env = "x"
    
     


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--gcd', 
                        help="GCD File", 
                        type=str, 
                        required=True)
    parser.add_argument("--files", 
                        help="", 
                        type="+",
                        required=True)
    parser.add_argument("--outdir", 
                        help="",
                        type=str,
                        required=True)
    parser.add_argument("--numcpus", 
                        help="",
                        type=int,
                        default=5)


    command = f"ls -la"
    commands = [
        f"{command} {i}" for i in args.files] # Example commands
    run_commands_parallel(commands, args.numcpus)
    print("All processes finished")
