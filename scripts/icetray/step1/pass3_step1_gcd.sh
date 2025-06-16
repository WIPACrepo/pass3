#!/bin/bash
#---

# Initialize.  Print environment for bookkeeping and error diagnostics.

DATE_NOW="date +%F%a%T | sed s/-//g | sed s/://g"

echo "${0} Begin DATE_NOW = ` eval ${DATE_NOW} `"

echo pwd
pwd
echo "hostname -A"
hostname -A
echo "hostname -I"
hostname -I
echo "uname -a"
uname -a
echo "cat /etc/redhat-release"
cat /etc/redhat-release
echo printenv
printenv

#---

# Define driver scripts.

ENV="/cvmfs/icecube.opensciencegrid.org/py3-v4.4.1/icetray-env"
echo "${0} ` eval ${DATE_NOW} ` ENV = ${ENV}"

BASEDIR="/data/user/i3filter/rsnihur/icetray/p3-v4.4.1_RHEL_7_audit_gcd_20250603"
echo "${0} ` eval ${DATE_NOW} ` BASEDIR = ${BASEDIR}"

SCRIPT="/data/user/i3filter/IC86_OfflineProcessing/OfflineSubmitScripts_pass3step1gcd_test/pass3_update_gcd_chargecorr.32.py"
echo "${0} ` eval ${DATE_NOW} ` SCRIPT = ${SCRIPT}"

SCRATCH=$_CONDOR_SCRATCH_DIR
if [ "x$SCRATCH" = "x" ]; then
  SCRATCH=/scratch
fi
echo "${0} ` eval ${DATE_NOW} ` SCRATCH = ${SCRATCH}"

#---

# Construct the names of all the directories and files needed for input and output data, standard output, and standard error.  
# Everything is based on a single input argument: the filename of the historical GCD file to be injected 
# with values of 1 for the mean ATWD and FADC charge correction.
# There are two possible input filename formats, for pass2b and post-pass2b:
# /data/ana/IceCube/2014/filtered/level2pass2b/0101/Run00123616/Level2pass2b_IC86.2013_data_Run00123616_0101_1_70_GCD.i3.zst
# /data/exp/IceCube/2024/filtered/dev/off.3/0101/Run00138808/Off_IC86.2023_data_Run00138808_0101_84_781_GCD.i3.zst

INPUT=$1
echo "${0} ` eval ${DATE_NOW} ` INPUT = ${INPUT}"

INDIR="$(dirname $INPUT)"
echo "${0} ` eval ${DATE_NOW} ` INDIR = ${INDIR}"

INFILE="$(basename $INPUT)"
echo "${0} ` eval ${DATE_NOW} ` INFILE = ${INFILE}"

INTARGET="`readlink ${INPUT}`"
echo "${0} ` eval ${DATE_NOW} ` INTARGET = ${INTARGET}"

RUN=`echo ${INDIR} | awk -Fn '{print $NF}' | sed 's/^0*//'`
echo "${0} ` eval ${DATE_NOW} ` RUN = ${RUN}"

# Handle pass2b case
OUTDIR="`echo ${INDIR} | sed s%/level2pass2b/%/OnlinePass3.5/% `"
echo "${0} ` eval ${DATE_NOW} ` OUTDIR = ${OUTDIR}"

# Also handle new offline case
OUTDIR="`echo ${OUTDIR} | sed s%/dev/off.3/%/OnlinePass3.5/% `"
echo "${0} ` eval ${DATE_NOW} ` OUTDIR = ${OUTDIR}"


OUTDIR="`echo ${OUTDIR} | sed s%/data/exp%/data/ana% `"
echo "${0} ` eval ${DATE_NOW} ` OUTDIR = ${OUTDIR}"

OUTPUT="${OUTDIR}/$(basename $INPUT)"
echo "${0} ` eval ${DATE_NOW} ` OUTPUT = ${OUTPUT}"

# Handle pass2b case
OUTPUT="`echo ${OUTPUT} | sed s%Level2pass2b_%OnlinePass3_% `"
echo "${0} ` eval ${DATE_NOW} ` OUTPUT = ${OUTPUT}"

# Also handle new offline case
OUTPUT="`echo ${OUTPUT} | sed s%Off_%OnlinePass3_% `"
echo "${0} ` eval ${DATE_NOW} ` OUTPUT = ${OUTPUT}"

OUTPUT="`echo ${OUTPUT} | sed s%".i3.gz"%".i3.zst"% `"
echo "${0} ` eval ${DATE_NOW} ` OUTPUT = ${OUTPUT}"

# Handle pass2b case 
OUTTARGET="`echo ${INTARGET} | sed s%Level2pass2b_%OnlinePass3_% `"
echo "${0} ` eval ${DATE_NOW} ` OUTTARGET = ${OUTTARGET}"

# Also handle new offline case 
OUTTARGET="`echo ${OUTTARGET} | sed s%Off_%OnlinePass3_% `"
echo "${0} ` eval ${DATE_NOW} ` OUTTARGET = ${OUTTARGET}"

# Also handle new offline case
OUTTARGET="`echo ${OUTTARGET} | sed s%../off.0/OfflinePreChecks/DataFiles/....%GCD% `"
echo "${0} ` eval ${DATE_NOW} ` OUTTARGET = ${OUTTARGET}"

OUTTARGET="`echo ${OUTTARGET} | sed s%".i3.gz"%".i3.zst"% `"
echo "${0} ` eval ${DATE_NOW} ` OUTTARGET = ${OUTTARGET}"

OUTLOG="$(dirname $OUTTARGET)/$(basename $OUTTARGET i3.zst)out"
echo "${0} ` eval ${DATE_NOW} ` OUTLOG = ${OUTLOG}"

OUTLOG="`echo ${OUTLOG} | sed s%/GCD/%/GCD_logs/% `"
echo "${0} ` eval ${DATE_NOW} ` OUTLOG = ${OUTLOG}"

ERRLOG="$(dirname $OUTTARGET)/$(basename $OUTTARGET i3.zst)err"
echo "${0} ` eval ${DATE_NOW} ` ERRLOG = ${ERRLOG}"

ERRLOG="`echo ${ERRLOG} | sed s%/GCD/%/GCD_logs/% `"
echo "${0} ` eval ${DATE_NOW} ` ERRLOG = ${ERRLOG}"

INAUDIT="$(dirname $OUTTARGET)/$(basename $OUTTARGET i3.zst)inaudit"
echo "${0} ` eval ${DATE_NOW} ` INAUDIT = ${INAUDIT}"

INAUDIT="`echo ${INAUDIT} | sed s%/GCD/%/GCD_logs/% `"
echo "${0} ` eval ${DATE_NOW} ` INAUDIT = ${INAUDIT}"

OUTAUDIT="$(dirname $OUTTARGET)/$(basename $OUTTARGET i3.zst)outaudit"
echo "${0} ` eval ${DATE_NOW} ` OUTAUDIT = ${OUTAUDIT}"

OUTAUDIT="`echo ${OUTAUDIT} | sed s%/GCD/%/GCD_logs/% `"
echo "${0} ` eval ${DATE_NOW} ` OUTAUDIT = ${OUTAUDIT}"


#---

# Next section creates the directories and symbolic links

CMD="mkdir -p ${OUTDIR}"
echo ${CMD}
${CMD}
STATUS=$?
echo "$0 ` eval ${DATE_NOW} ` ${CMD} STATUS = ${STATUS}"

if [ ${STATUS} -ne 0 ]; then
    echo "$0 ` eval ${DATE_NOW} ` exiting"
    exit ${STATUS}
fi

CMD="cd ${OUTDIR}"
echo ${CMD}
${CMD}
STATUS=$?
echo "$0 ` eval ${DATE_NOW} ` ${CMD} STATUS = ${STATUS}"

if [ ${STATUS} -ne 0 ]; then
    echo "$0 ` eval ${DATE_NOW} ` exiting"
    exit ${STATUS}
fi

CMD="mkdir -p $(dirname ${OUTTARGET})"
echo ${CMD}
${CMD}
STATUS=$?
echo "$0 ` eval ${DATE_NOW} ` ${CMD} STATUS = ${STATUS}"

if [ ${STATUS} -ne 0 ]; then
    echo "$0 ` eval ${DATE_NOW} ` exiting"
    exit ${STATUS}
fi

# Delete OUTPUT file if it exists
CMD="rm -f ${OUTPUT}"
echo ${CMD}
${CMD}
STATUS=$?
echo "$0 ` eval ${DATE_NOW} ` ${CMD} STATUS = ${STATUS}"

CMD="ln -sf ${OUTTARGET} $(basename $OUTPUT)"
echo ${CMD}
${CMD}
STATUS=$?
echo "$0 ` eval ${DATE_NOW} ` ${CMD} STATUS = ${STATUS}"

if [ ${STATUS} -ne 0 ]; then
    echo "$0 ` eval ${DATE_NOW} ` exiting"
    exit ${STATUS}
fi

CMD="mkdir -p $(dirname ${OUTLOG})"
echo ${CMD}
${CMD}
STATUS=$?
echo "$0 ` eval ${DATE_NOW} ` ${CMD} STATUS = ${STATUS}"

if [ ${STATUS} -ne 0 ]; then
    echo "$0 ` eval ${DATE_NOW} ` exiting"
    exit ${STATUS}
fi

CMD="mkdir -p $(dirname ${ERRLOG})"
echo ${CMD}
${CMD}
STATUS=$?
echo "$0 ` eval ${DATE_NOW} ` ${CMD} STATUS = ${STATUS}"

if [ ${STATUS} -ne 0 ]; then
    echo "$0 ` eval ${DATE_NOW} ` exiting"
    exit ${STATUS}
fi

# Delete OUTLOG file if it exists
CMD="rm -f ${OUTLOG}"
echo ${CMD}
${CMD}
STATUS=$?
echo "$0 ` eval ${DATE_NOW} ` ${CMD} STATUS = ${STATUS}"

# Delete ERRLOG file if it exists 
CMD="rm -f ${ERRLOG}"
echo ${CMD}
${CMD}
STATUS=$?
echo "$0 ` eval ${DATE_NOW} ` ${CMD} STATUS = ${STATUS}"

# Delete INAUDIT file if it exists
CMD="rm -f ${INAUDIT}"
echo ${CMD}
${CMD}
STATUS=$?
echo "$0 ` eval ${DATE_NOW} ` ${CMD} STATUS = ${STATUS}"

# Delete OUTAUDIT file if it exists
CMD="rm -f ${OUTAUDIT}"
echo ${CMD}
${CMD}
STATUS=$?
echo "$0 ` eval ${DATE_NOW} ` ${CMD} STATUS = ${STATUS}"

#---

# Next section marks this run as started with timestamp in DB. Exit if this run is already in DB.

PASS=`cat /data/user/i3filter/IC86_OfflineProcessing/OfflineSubmitScripts_pass3step1gcd_test/filter-db.config`

mysql -N i3filter --host=filter-db --user=i3filter --password=${PASS} << EOF
insert into onlinepass3_gcd_processing (run_id, path, done, date) values (${RUN}, "${INPUT}", 0, NOW());
EOF
STATUS=$?
echo "$0 ` eval ${DATE_NOW} ` mysql insert STATUS = ${STATUS}"

if [ ${STATUS} -ne 0 ]; then
    echo "$0 ` eval ${DATE_NOW} ` exiting"
    exit ${STATUS}
fi

#---

# Main section

CMD="${ENV} ${BASEDIR} python ${SCRIPT} ${INPUT} ${OUTTARGET} ${INAUDIT} ${OUTAUDIT}"
echo ${CMD}

# Append to output and error log files for this step.
${CMD} 1>>${OUTLOG} 2>>${ERRLOG}
STATUS=$?
echo "$0 ` eval ${DATE_NOW} ` ${CMD} STATUS = ${STATUS}"

if [ ${STATUS} -ne 0 ]; then
    echo "$0 ` eval ${DATE_NOW} ` exiting"
    exit ${STATUS}
fi

#---

# Validation section

SCRIPT="${BASEDIR}/dataio/resources/examples/scan.py -c"
echo "${0} ` eval ${DATE_NOW} ` SCRIPT = ${SCRIPT}"

CMD="${ENV} ${BASEDIR} ${SCRIPT} ${OUTPUT}"
echo ${CMD}
# NB append to BOTH output and error log files for this step.
${CMD} |& tee -a ${OUTLOG} ${ERRLOG}
STATUS=$?
echo "$0 ` eval ${DATE_NOW} ` ${CMD} STATUS = ${STATUS}"

if [ ${STATUS} -ne 0 ]; then
    echo "$0 ` eval ${DATE_NOW} ` exiting"
    exit ${STATUS}
fi

# Confirm correct frame count from scan
CMD="grep 4\sframes ${ERRLOG}"
echo ${CMD}
${CMD}
STATUS=$?
echo "$0 ` eval ${DATE_NOW} ` ${CMD} STATUS = ${STATUS}"

if [ ${STATUS} -ne 0 ]; then
    echo "$0 ` eval ${DATE_NOW} ` exiting"
    exit ${STATUS}
fi

# Mark this run done in DB
mysql -N i3filter --host=filter-db --user=i3filter --password=${PASS} << EOF
update onlinepass3_gcd_processing set done = 1, date = NOW() where run_id = ${RUN} and path = "${INPUT}";
EOF
STATUS=$?
echo "$0 ` eval ${DATE_NOW} ` mysql update STATUS = ${STATUS}"

if [ ${STATUS} -ne 0 ]; then
    echo "$0 ` eval ${DATE_NOW} ` exiting"
    exit ${STATUS}
fi


exit 0
