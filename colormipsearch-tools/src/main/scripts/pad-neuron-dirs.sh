#!/bin/bash

DIR=$(cd "$(dirname "$0")"; pwd)

read RESDIR
read NEURON_DIRS
neuronDirs=(${NEURON_DIRS[@]})

CONCURRENCY=${CONCURRENCY:-1}

echo "Process $neuronDirs"

function process_neuron_dir {
    local neuronDir=$1

    declare -a pids=()
    local neuronName=$(basename ${neuronDir})
    local neuronPartition=${neuronName:0:2}

    echo "Process $neuronDir"
    mkdir -p ${RESDIR}/${neuronPartition}/${neuronName}

    local nfiles=0
    for fn in ${neuronDir}/* ; do
	echo ${DIR}/pad-single-pppimage.sh ${fn} ${RESDIR}/${neuronPartition}/${neuronName}
	${DIR}/pad-single-pppimage.sh ${fn} ${RESDIR}/${neuronPartition}/${neuronName} &
	pid=$!
	pids=("${pids[@]}" $pid)
	npids=${#pids[@]}
	nfiles=$((nfiles+1))
	if [[ ${npids} -ge ${CONCURRENCY} ]]; then
	    echo "Wait for $npids to complete (already submitted $nfiles)"
	    wait ${pids[@]}
	    pids=()
	fi
    done
    npids=${#pids[@]}
    if [[ ${npids} -gt 0 ]]; then
	echo "Wait for last $npids to complete"
	wait ${pids[@]}
	pids=()
    fi
}

for neuronDir in ${neuronDirs[@]} ; do 
    process_neuron_dir ${neuronDir}
done
