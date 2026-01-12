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

ENV="/cvmfs/icecube.opensciencegrid.org/py3-v4.4.2"
echo "${0} ` eval ${DATE_NOW} ` ENV = ${ENV}"

BASEDIR="icetray/v1.15.3"
echo "${0} ` eval ${DATE_NOW} ` BASEDIR = ${BASEDIR}"

SCRIPT="/data/user/i3filter/IC86_OfflineProcessing/OfflineSubmitScripts_pass3step2gcd_leapfix_test/src/pass3/leapfix/scripts/icetray/step2gcd/leapfix_gcdfile.py"
echo "${0} ` eval ${DATE_NOW} ` SCRIPT = ${SCRIPT}"

SCRATCH=$_CONDOR_SCRATCH_DIR
if [ "x$SCRATCH" = "x" ]; then
  SCRATCH=/scratch
fi
echo "${0} ` eval ${DATE_NOW} ` SCRATCH = ${SCRATCH}"

#---

# Construct the names of all the directories and files needed for input and output data, standard output, and standard error.  
# Everything is based on a single input argument: the filename of the pass3 step2 historical GCD file
# which will have the extra leap second subtracted from the D-frame keys GoodRunStartTime and GoodRunEndTime.
# This is the input filename format:
# /data/ana/IceCube/2017/filtered/OfflinePass3.0/0301/Run00129239/OfflinePass3_IC86.2016_data_Run00129239_0301_90_299_GCD.i3.zst

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

# Handle OfflinePass3.0 case
OUTDIR="`echo ${INDIR} | sed s%/OfflinePass3.0/%/OfflinePass3.2/% `"
echo "${0} ` eval ${DATE_NOW} ` OUTDIR = ${OUTDIR}"

OUTPUT="${OUTDIR}/$(basename $INPUT)"
echo "${0} ` eval ${DATE_NOW} ` OUTPUT = ${OUTPUT}"

# Handle OnlinePass3 case
#OUTPUT="`echo ${OUTPUT} | sed s%OnlinePass3_%OfflinePass3_% `"
#echo "${0} ` eval ${DATE_NOW} ` OUTPUT = ${OUTPUT}"

OUTPUT="`echo ${OUTPUT} | sed s%".i3.gz"%".i3.zst"% `"
echo "${0} ` eval ${DATE_NOW} ` OUTPUT = ${OUTPUT}"

# Handle any case 
#OUTTARGET="`echo ${INTARGET} | sed s%OnlinePass3_%OfflinePass3_% `"
OUTTARGET=${INTARGET}
echo "${0} ` eval ${DATE_NOW} ` OUTTARGET = ${OUTTARGET}"

OUTTARGET="`echo ${OUTTARGET} | sed s%".i3.gz"%".i3.zst"% `"
echo "${0} ` eval ${DATE_NOW} ` OUTTARGET = ${OUTTARGET}"

OUTLOG="$(dirname $OUTTARGET)/$(basename $OUTTARGET i3.zst)out"
echo "${0} ` eval ${DATE_NOW} ` OUTLOG = ${OUTLOG}"

OUTLOG="`echo ${OUTLOG} | sed s%/GCD/%/GCD_logs/% `"
echo "${0} ` eval ${DATE_NOW} ` OUTLOG = ${OUTLOG}"

ERRLOG="`echo ${OUTLOG} | sed s%.out%.err% `"
echo "${0} ` eval ${DATE_NOW} ` ERRLOG = ${ERRLOG}"

PYLOG="`echo ${OUTLOG} | sed s%.out%.log% `"
echo "${0} ` eval ${DATE_NOW} ` PYLOG = ${PYLOG}"

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

CMD="mkdir -p $(dirname ${PYLOG})"
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

# Delete PYLOG file if it exists
CMD="rm -f ${PYLOG}"
echo ${CMD}
${CMD}
STATUS=$?
echo "$0 ` eval ${DATE_NOW} ` ${CMD} STATUS = ${STATUS}"

#---

# Next section marks this run as started with timestamp in DB. Exit if this run is already in DB.

PASS=`cat /home/i3filter/mysql-2.config`

mysql -v -N i3filter --host=mysql-2 --user=i3filter --password=${PASS} << EOF
insert into offlinepass3_leapfix_gcd_processing (run_id, path, done, date) values (${RUN}, "${INPUT}", 0, NOW());
EOF
STATUS=$?
echo "$0 ` eval ${DATE_NOW} ` mysql insert STATUS = ${STATUS}"

if [ ${STATUS} -ne 0 ]; then
    echo "$0 ` eval ${DATE_NOW} ` exiting"
    exit ${STATUS}
fi

#---

# Main section

CMD="${ENV}/icetray-env ${BASEDIR} python ${SCRIPT} -i ${INPUT} -o ${OUTTARGET}"
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

SCRIPT="${ENV}/metaprojects/${BASEDIR}/dataio/resources/examples/scan.py -c"
echo "${0} ` eval ${DATE_NOW} ` SCRIPT = ${SCRIPT}"

CMD="${ENV}/icetray-env ${BASEDIR} python ${SCRIPT} ${OUTPUT}"
echo ${CMD}
# NB append to BOTH output and error log files for this step.
#${CMD} |& tee -a ${OUTLOG} ${ERRLOG}
${CMD} |& tee -a ${ERRLOG}
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
mysql -N i3filter --host=mysql-2 --user=i3filter --password=${PASS} << EOF
update offlinepass3_leapfix_gcd_processing set done = 1, date = NOW() where run_id = ${RUN} and path = "${INPUT}";
EOF
STATUS=$?
echo "$0 ` eval ${DATE_NOW} ` mysql update STATUS = ${STATUS}"

if [ ${STATUS} -ne 0 ]; then
    echo "$0 ` eval ${DATE_NOW} ` exiting"
    exit ${STATUS}
fi


exit 0
