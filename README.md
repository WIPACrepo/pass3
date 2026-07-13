# pass3

Notes, Scripts, etc. regarding Pass3

# Step 1 at TACC

### Building the Container

Step 1 runs in an `apptainer` container. Mostly because we want a
CVMFS-like environment, but we don't have ARM builds right now. We 
couldn't use the `main` py4.4.2 cvmfs/spack install because 
HTCondor pybindings versions get deleted from pypi. Trying to build a 
container from scratch will require changing the HTCondor pybindings 
version to one that is available. This is needed because of limited 
space on pypi. There is a branch `pass3` in the cvmfs repo that allows 
us to compensate for this. 

## Getting Data from Tape

The workflow is roughly the same on both Stampede3 and Vista. The main
difference is the size of the scratch disk (located at `$SCRATCH`) and
the location where the `$WORK` variable points.

On Ranch, the data is located at 
`/scoutfs/projects/TG-PHY150040/data/exp/IceCube/<YYYY>/unbiased/PFRaw/`

for 2011-2014 data you will want to use the

`/scoutfs/projects/TG-PHY150040/OldRanchData/data/exp/IceCube/<YYYY>/unbiased/PFRaw/`

until the transition to LTO-10 tapes has been made. This is scheduled for later
in 2026. The files in `/scoutfs/projects/TG-PHY150040/data/exp/IceCube/<2011-2014>/` 
are spread across several tapes (usually number of tapes = number of 
tape drives available at the time). Pulling from several tapes to 
get the files is painfully slow compared to using `OldRanchData`.

When the code was originally written the tape archive was fast enough
and the bundles small enough that one could transfer the data at the
start of the job. With larger bundles in earlier years (pre-2019) and
a transition to a "new" Ranch (marked by the transition to scoutfs),
the transfer speeds degraded to the point that pre-staging the data is
required. A large part of the decrease in speed on Ranch was that "new"
Ranch only has 12 tape drives. This is supposed to be upped to 24 (or
even 40) drives down the road. For right now though, pre-staging is
best.

To transfer data from Ranch to either scratch disk there are two methods:

1) `rsync`

Using

`rsync -aviP $ARCHIVER:/scoutfs/projects/TG-PHY150040/data/exp/IceCube/YYYY/unbiased/PFRaw/MM* $SCRATCH/<your_data_dir>/YYYY/`

This is how this looks for me:

`rsync -aviP $ARCHIVER:/scoutfs/projects/TG-PHY150040/data/exp/IceCube/2024/unbiased/PFRaw/09* /scratch/04799/tg840985/tmp.v2/2024/`

`$ARCHIVER` is `ranch.tacc.utexas.edu`

2) Globus

To set up your account for Globus at TACC go to 
[https://docs.tacc.utexas.edu/datatransfer/globus/](https://docs.tacc.utexas.edu/datatransfer/globus/)

Using Globus (web interface, CLI, python API) you can transfer from
Ranch collection (`TACC Ranch3 GCS v5.4 Tape Archive`,
`57c4032a-2b50-47f0-adf8-13fff3a7d77d`) to the Stampede3
(`TACC Stampede3 GCS v5.4 Filesystems`,
`1e9ddd41-fe4b-406f-95ff-f3d79f9cb523`) or
Vista (`TACC Vista GCS v5.4 Filesystems`,
`fcb0b578-dcb3-4043-a841-8bd0974d6af1`) collection.

I would recommend `rsync` because you can run 4-6 transfers in parallel. 
Globus only allows 3 transfers in parallel at the moment


## Generating submit files

`submit_stampede3.py` (located in `pass3/scripts/submit/step1/`) is 
setup to generate a SLURM submit file that than uses `srun` multiprog 
files. Multiprog files is a list of commands that should be run on the 
node that `srun` assigns it to. The one advantage of multiprog files 
is that `srun` does the per node assignment , so you don't have to manually
assign things to different nodes, i.e. makes things more legible and 
understandable.


The scripts are setup to work in month data increments. Once you have 
transferred a month worth data to your temporary location 

```
YEAR=2024; MONTH=2; python3 $HOME/pass3/scripts/submit/step1/submit_stampede3.py --container /scratch/04799/tg840985/pass3_cvmfs_v4.4.2_icetray_v1.17.0_arm.sif --scratchdir /scratch/04799/tg840985/tmp_scratch/ --submitfile /home1/04799/tg840985/pass3/submit_files/${YEAR}/${YEAR}_${MONTH}_new_gcd.slurm --checksum-file /home1/04799/tg840985/pass3/data/checksums.sha512sum --year ${YEAR} --month ${MONTH} --gcddir /work/04799/tg840985/stampede3/GCD.v2/ --multiprogfile /home1/04799/tg840985/pass3/submit_files/${YEAR}/${YEAR}_${MONTH}_new_gcd.multiprog --slurmqueue gg --numcores 100 --allocation PHY20012 --grl /home1/04799/tg840985/pass3/data/grl.pass3 --badfiles /home1/04799/tg840985/pass3/data/known_bad_files --cpuarch aarch64 --bundlesready --bundledir $SCRATCH/tmp.v2/ --outdir $SCRATCH/testout.v2/${YEAR} --duplicate-skip-dir /home1/04799/tg840985/pass3/submit_files/${YEAR}/skip/
```

An option to note is

`--duplicate-skip-dir /home1/04799/tg840985/pass3/submit_files/${YEAR}/skip/`

this will look for duplicates across the contents of different bundles.
There are bundles that contain the same file. This internal algorithm
picks one of the two duplicates to process and ignores the other.

On Vista, we need to limit the number of cores. There are total of 144 cores
and 237 GB per Grace-Grace node. The code uses ~2.2 GB RAM per instance.

A sample of a SLURM submit file is:

```
#!/bin/bash
#SBATCH -t 24:00:00
#SBATCH -A PHY20012
#SBATCH -p gg
#SBATCH -J /home1/04799/tg840985/pass3/submit_files/2022/2022_1_new_gcd.slurm
#SBATCH -N 32
#SBATCH -n 32
#SBATCH -o /home1/04799/tg840985/pass3/submit_files/2022/2022_1_new_gcd.slurm.o.%j
#SBATCH -e /home1/04799/tg840985/pass3/submit_files/2022/2022_1_new_gcd.slurm.e.%j

echo `date`

LD_PRELOAD=

if [ ! -e /home1/04799/tg840985/pass3/submit_files/2022/2022_1_new_gcd.multiprog0.done ]; then
echo Starting /home1/04799/tg840985/pass3/submit_files/2022/2022_1_new_gcd.multiprog0
echo `date`
srun --nodes=32 --ntasks-per-node=1 --exclusive --cpus-per-task=$SLURM_CPUS_ON_NODE --multi-prog /home1/04799/tg840985/pass3/submit_files/2022/2022_1_new_gcd.multiprog0 && touch /home1/04799/tg840985/pass3/submit_files/2022/2022_1_new_gcd.multiprog0.done || touch /home1/04799/tg840985/pass3/submit_files/2022/2022_1_new_gcd.multiprog0.failed
fi
```

We have `LD_PRELOAD=` to avoid some errors with `apptainer`. The `*.done`
and `*.failed` files are empty. They are just indicators whether the 
`srun` command has succeed or not. Failure of a "program" from the 
multiprog file will also make `srun` fail.

The smallest good processing increment is a bundle. Each "program" will 
take a bundle (and necessary information for said bundle) and process it.

An example "program" line for multiprog file:

```
0  /opt/apps/tacc-apptainer/1.3.3/bin/apptainer exec -B /home1/04799/tg840985/pass3:/opt/pass3 -B /work/04799/tg840985/vista/splines/splines:/cvmfs/icecube.opensciencegrid.org/data/photon-tables/splines -B /work2 -B /scratch /scratch/04799/tg840985/pass3_cvmfs_v4.4.2_icetray_v1.17.0_arm.sif /cvmfs/icecube.opensciencegrid.org/py3-v4.4.2/RHEL_9_aarch64/metaprojects/icetray/v1.17.0/bin/icetray-shell python3 /opt/pass3/scripts/icetray/step1/run_step1.py --bundle /scratch/04799/tg840985/tmp.v2/2022/0101/24a633c4f11c11ecb5b232208fe3aaeb.zip --gcddir /work2/04799/tg840985/stampede3/GCD.v2 --outdir /scratch/04799/tg840985/testout.v2/2022/0101 --checksum 3e08159c72bb6e7a37aaeb6ae0389ba8aa6d98c14b90e9296c6e77f8544ebb4f9bb241a479443b4707e5f53db4bbe9a542774a98ad197907f0dad2b645e40254 --scratchdir /scratch/04799/tg840985/tmp.v2/2022 --grl /home1/04799/tg840985/pass3/data/grl.pass3 --badfiles /home1/04799/tg840985/pass3/data/known_bad_files  --duplicate-skip-json /home1/04799/tg840985/pass3/submit_files/2022/skip/24a633c4f11c11ecb5b232208fe3aaeb.zip.duplicate_skip.json --maxnumcpus 100
```

` -B /home1/04799/tg840985/pass3:/opt/pass3`: Gets the pass3 code pieces into the container
`-B /work/04799/tg840985/vista/splines/splines:/cvmfs/icecube.opensciencegrid.org/data/photon-tables/spline`:  the necessary splines for the realtime filter:

One key aspect is that number of nodes passed to `srun` needs to match 
the number of "programs" in the multiprog file, so for some years and 
months there are padding. We waste some resources but in the grant 
scheme it is okay. 

### Submitting Jobs

Now just `sbatch <submit_file>` you generated with `submit_stampede3.py`.

### What does the job/"program" do?

On a high level, the job itself runs `run_step1.py` (located in 
`pass3/scripts/icetray/step1/`) on each node. `run_step1.py` takes the
files in the bundle and applies `pass3_reprocess_PFRaw.py` to the `PFRaw`
files in the bundle. The bundles only contain a manifest file (earlier 
years it is a `*.json` file with a single json object for all file, in 
later years it is `*.ndjson` with each file being on a new line as a 
json object). The `PFRaw` files end in `*.tar.gz`. 

The `concurrent.futures.ProcessPoolExecutor` allows a mapping list of 
inputs to N workers (each being a Python `subprocess`) and monitors the 
state of each worker. This bypasses the GIL for "real" multi-processing. 
For each `runner`, we prepare the required inputs (directory to find GCD 
files, path to the archival bundle, PFRaw file, directory to place 
outputs, expected SHA512 checksum for the PFRaw file from the archive 
bundle). The input preparation includes making sure that the bundle 
SHA512 is correct. It will also check whether some or all of the 
`PFRaw` files in a bundle should be skipped because they appear in a 
different bundle. There are some bundles that are complete duplicates 
of others, so there will be an error that there are no inputs.

After the inputs are checked and the list of inputs is passed to 
`ProcessPoolExecutor`. The `runner` will:

1) Create a temporary working directory
2) Find the GCD file for the run from the PFRaw file name - Issues a warning and returns if it can't be found
3) Ensure the GCD file has the expected quantities (`pass3_check_gcd.py`) - FADC gains are what we expect (Each GCD file contains a map of DOM to original FADC value. 
). mean ATWD, FADC charge = 1, relative DOM efficiency is not NAN.
4) Extract the PFRaw file
5) Check the checksum for the PFRaw file - Issues a warning and returns it isn't what we expect
6) Check if the outfile already exsits - Issues a warning and returns if it does. To replace an output file you need to rename or delete it.
7) Run `pass3_reprocess_PFRaw.py`
8) Run monitoring scripts (`pass3_calc_filter_rates.py` and `pass3_check_charge_filter.py`)
9) Calculate the SHA512 checksum for the output file
10) Moves all outputs from the temporary working directory to the output dir

Steps 6 through 8 log into separate files that will be colocated with 
the output files.

The `runner` will generally not raise Exceptions. This is to allow for 
better handling of errors and logging. The `ProcessPoolExecutor` 
fails when one of the workers fails. This causes unprocessed/subsequent 
PFRaw files to not be processed. Checking the return values allows us 
to summarize what has failed. 


### Transferring Output

The output is best tranferred using Globus. Transferring a whole year is best given the directory structure


