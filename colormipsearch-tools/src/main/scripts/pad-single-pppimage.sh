#!/bin/bash

function process_raw {
    fullfn=$1
    resdir=$2

    fn=`basename "${fullfn}"`
    echo "Process raw $fullfn -> $resdir/$fn"

    convert "$fullfn" -background black -splice 0x90 -gravity North "${resdir}/${fn}"
}

function process_ch {
    fullfn=$1
    resdir=$2

    fn=`basename "${fullfn}"`
    echo "Process ch $fullfn -> $resdir/$fn"

    convert "$fullfn" -background black -splice 0x90 -gravity North "${resdir}/${fn}"
}


function process_skel {
    fullfn=$1
    resdir=$2

    fn=`basename "${fullfn}"`
    echo "Process skel $fullfn -> $resdir/$fn"

    convert "$fullfn" -background "rgb(100,100,100)" -splice 0x90 -gravity North "${resdir}/${fn}"
}


function process_ppp_image {
    fullfn=$1
    resdir=$2

    case "${fullfn}" in
	*-ch.png)
	    process_ch "${fullfn}" ${resdir}
	    ;;
	*-ch_skel.png)
	    process_ch "${fullfn}" ${resdir}
	    ;;
	*-masked_raw.png)
	    process_skel "${fullfn}" ${resdir}
	    ;;
	*-skel.png)
	    process_skel "${fullfn}" ${resdir}
	    ;;
	*-raw.png)
	    process_raw "${fullfn}" ${resdir}
	    ;;
	*)
	    echo "Unsupported file type: ${fullfn}"
	    exit 1
	    ;;
    esac
}

input="$1"
outputdir="$2"

process_ppp_image "$input" $outputdir
