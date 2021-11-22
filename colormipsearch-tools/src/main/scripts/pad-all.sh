#!/bin/bash

DIR=$(cd "$(dirname "$0")"; pwd)

NEURON_P1_PARTITIONS=(
10 11 12 13 14 15 16 17 18 19
20 21 22 23 24 25 26 27 28 29
30 31 32 33 34 35 36 37 38 39
40 41 42 43 44 45 46 47 48 49
50 51 52 53 54 55 56 57 58 59
60 61 62 63 64 65 66 67 68 69
70 71 72 73 74 75 76 77 78 79
80 81 82 83 84 85 86 87 88 89
90 91 92 93 94 95 96 97 98 99
)

NEURON_P2_PARTITIONS=(
00 01 02 03 04 05 06 07 08 09
10 11 12 13 14 15 16 17 18 19
20 21 22 23 24 25 26 27 28 29
30 31 32 33 34 35 36 37 38 39
40 41 42 43 44 45 46 47 48 49
50 51 52 53 54 55 56 57 58 59
60 61 62 63 64 65 66 67 68 69
70 71 72 73 74 75 76 77 78 79
80 81 82 83 84 85 86 87 88 89
90 91 92 93 94 95 96 97 98 99
)

NEURON_P1_PARTITIONS=(
10
)

NEURON_P2_PARTITIONS=(
06
09
10
22
31
57
58
70
93
)

SRCDIR=/nrs/neuronbridge/ppp_imagery/v2.3.0-pre/FlyEM_VNC_v0.6
RESDIR=/nrs/neuronbridge/ppp_imagery/v2.3.0-pre-resized/FlyEM_VNC_v0.6
NCORES=32
RUNCMD=${RUNCMD:-runLocal}

export CONCURRENCY=${CONCURRENCY:-${NCORES}}

p2From=0
p2To=$((${#NEURON_P2_PARTITIONS[@]}-1))

function createInputParams {
# there's no indentation here on purpose because of the cat
local p1=$1
local maxJobIndex=$((${#NEURON_P2_PARTITIONS[@]}-1))

for ((jobIndex=${p2From}; jobIndex<=${p2To} && jobIndex<=$((maxJobIndex)); jobIndex++)) ; do
cat > ${DIR}/inputs/RESIZE_${p1}_$((jobIndex+1)) <<EOF
${RESDIR}
$SRCDIR/${p1}/${p1}${NEURON_P2_PARTITIONS[${jobIndex}]}*
EOF
done
}

function runLocal {
    local p1=$1
    local maxJobIndex=$((${#NEURON_P2_PARTITIONS[@]}-1))

    createInputParams $p1

    for ((jobIndex=${p2From}; jobIndex<=${p2To} && jobIndex<=$((maxJobIndex)); jobIndex++)) ; do
	${DIR}/pad-neuron-dirs.sh < ${DIR}/inputs/RESIZE_${p1}_$((jobIndex+1)) > ${DIR}/logs/output_${jobIndex}.out
    done
}

function runGrid {
    local p1=$1
    local maxJobIndex=$((${#NEURON_P2_PARTITIONS[@]}-1))

    createInputParams $p1
 
    JOBNAME="RESIZE_$p1[$((p2From+1))-$((p2To+1))]"

    bsub \
	-n ${NCORES} \
	-J ${JOBNAME} \
	-o ${DIR}/logs/%J_%I.out -e ${DIR}/logs/%J_%I.err \
	-i ${DIR}/inputs/RESIZE_${p1}_%I \
	-P neuronbridge \
	${DIR}/pad-neuron-dirs.sh
}

for p1 in ${NEURON_P1_PARTITIONS[@]} ; do
    ${RUNCMD} $p1
done
