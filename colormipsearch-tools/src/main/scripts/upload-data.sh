S3_BUCKET=janelia-neuronbridge-data-devpre
S3_DATA_VERSION=v3_0_0
LOCAL_DATA_DIR=/nrs/neuronbridge/v3.0.0
MIPS_DIR=mips
CDS_RESULTS_DIR=cdmatches
PPPM_RESULTS_DIR=pppmatches
PER_EM_DIR=em-vs-lm
PER_LM_DIR=lm-vs-em
LM_MIPS=lmlines
EM_MIPS=embodies

AWS="echo aws"

AWSCP="$AWS s3 cp"

uploadMIPS() {
    local region="$1" # brain, vnc, brain+vnc, or vnc+brain
    local mips_type="$2" # lmlines or embodies
    local d=${LOCAL_DATA_DIR}/${region}/${MIPS_DIR}/${mips_type}
    local mips_dest

    case ${mips_type} in
        lmlines|lm_lines|by_line)
            mips_dest=by_line
            ;;
        embodies|by_body|em_bodies)
            mips_dest=by_body
            ;;
        *)
            echo "Unsupported mips type: ${mips_type}"
            exit 1
    esac

    # upload
    $AWSCP $d s3://${S3_BUCKET}/${S3_DATA_VERSION}/metadata/${mips_dest} --recursive
}

uploadMatches() {
    local region="$1" # brain or vnc
    local matches_type="$2" # cdmatches or pppmatches
    local direction="$3" # em-vs-lm or lm-vs-em

    local src_subdir
    local dest_subdir
    case ${matches_type} in
        cdm|cdmatches|cds|cdsresults)
            src_subdir=${CDS_RESULTS_DIR}
            dest_subdir=cdsresults
            ;;
        ppp|pppm|pppmatches|pppmresults)
            src_subdir=${PPPM_RESULTS_DIR}
            dest_subdir=pppmresults
            ;;
        *)
            echo "Unsupported matches type: ${matches_type}"
            exit 1
      esac
      local d=${LOCAL_DATA_DIR}/${region}/${src_subdir}/${direction}
      $AWSCP ${d} s3://${S3_BUCKET}/${S3_DATA_VERSION}/metadata/${dest_subdir} --recursive
}

uploadConfig() {
    $AWSCP ${LOCAL_DATA_DIR}/config.json s3://${S3_BUCKET}/${S3_DATA_VERSION}/config.json
}

uploadSchemas() {
    $AWSCP ${LOCAL_DATA_DIR}/schemas s3://${S3_BUCKET}/${S3_DATA_VERSION}/schemas --recursive
}

uploadVersion() {
    $AWSCP ${LOCAL_DATA_DIR}/current.txt s3://${S3_BUCKET}/current.txt
    $AWSCP ${LOCAL_DATA_DIR}/current.txt s3://${S3_BUCKET}/next.txt
}

uploadMIPS brain+vnc ${LM_MIPS}
uploadMIPS brain+vnc ${EM_MIPS}

uploadMatches brain cds ${PER_EM_DIR}
uploadMatches brain cds ${PER_LM_DIR}
uploadMatches brain pppm ${PER_EM_DIR}

uploadMatches vnc cds ${PER_EM_DIR}
uploadMatches vnc cds ${PER_LM_DIR}
uploadMatches vnc pppm ${PER_EM_DIR}
