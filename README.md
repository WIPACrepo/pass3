# pass3

Notes, Scripts, etc. regarding Pass3

# Step 1 at TACC

## Getting Data from Tape

The workflow is roughly the same on both Stampede3 and Vista. The main
difference is the size of the scratch disk (located at `$SCRATCH`) and
the location where the `$WORK` variable points.

On Ranch, the data is located at 
`/scoutfs/projects/TG-PHY150040/data/exp/IceCube/<YYYY>/unbiased/PFRaw/`

for 2011-2014 data you will want to use the

`/scoutfs/projects/TG-PHY150040/OldRanchData/data/exp/IceCube/<YYYY>/unbiased/PFRaw/`

until the transition to LTO-10 tapes has been made. This is scheduled for later
in 2026.

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

1) Rsync

Using

`rsync -aviP $ARCHIVER:/scoutfs/projects/TG-PHY150040/data/exp/IceCube/YYYY/unbiased/PFRaw/MM* $SCRATCH/<your_data_dir>/YYYY/`

This is how this looks for me:

`rsync -aviP $ARCHIVER:/scoutfs/projects/TG-PHY150040/data/exp/IceCube/2024/unbiased/PFRaw/09* /scratch/04799/tg840985/tmp.v2/2024/`

2) Globus

To set up your account for Globus at TACC go to [https://docs.tacc.utexas.edu/datatransfer/globus/](https://docs.tacc.utexas.edu/datatransfer/globus/)

Using Globus (web interface, CLI, python API) you can transfer from
Ranch collection (`TACC Ranch3 GCS v5.4 Tape Archive`,
`57c4032a-2b50-47f0-adf8-13fff3a7d77d`) to the Stampede3
(`TACC Stampede3 GCS v5.4 Filesystems`,
`1e9ddd41-fe4b-406f-95ff-f3d79f9cb523`) or
Vista (`TACC Vista GCS v5.4 Filesystems`,
`fcb0b578-dcb3-4043-a841-8bd0974d6af1`) collection.

I would recommend `rsync` because you can run 4-6 transfers in parallel. Globus only allows 3 transfers in parallel at the moment


## Generating submit files

`pass3/scripts/submit/step1/submit_stampede3.py` is setup to generate
a SLURM submit file that than uses `srun` multiprog files.
Multiprog files is a list of commands that should be run on the node that
`srun` assigns it to. The one advantage of

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

### Submitting Jobs

Now just `sbatch <submit_file>` you generated with `submit_stampede3.py`

### Transferring Output

The output is best tranferred using Globus. Transferring a whole year is best given the directory structure


