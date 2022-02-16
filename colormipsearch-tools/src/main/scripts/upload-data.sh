S3_BUCKET=janelia-neuronbridge-data-dev
S3_DATA_VERSION=v2_4_0
REGION=brain
LOCAL_VERSION_DATA_DIR=/nrs/neuronbridge/v2.4.0
LOCAL_CDS_DATA_DIR=/nrs/neuronbridge/v2.4.0
LOCAL_PPP_DATA_DIR=/nrs/neuronbridge/v2.4.0
PER_EM_DIR=flyem-vs-flylight
PER_LM_DIR=flylight-vs-flyem
CDS_RESULTS_DIR=${REGION}/cdsresults.final
MIPS_DIR=${REGION}/mips
MCFO_MIPS=all_mcfo_lines
SGAL4_MIPS=split_gal4_lines
EM_MIPS=em_bodies
PPP_RESULTS_DIR=${REGION}/pppresults/flyem-to-flylight.public

AWS="echo aws"

AWSCP="$AWS s3 cp"

uploadMCFOMIPs() {
    local d=${LOCAL_CDS_DATA_DIR}/${MIPS_DIR}/${MCFO_MIPS}
    if [ -d $d ]; then
        $AWSCP $d s3://${S3_BUCKET}/${S3_DATA_VERSION}/metadata/by_line --recursive
    else
        echo "uploadMCFOMIPs: $d not found"
    fi
}

uploadSGal4MIPs() {
    local d=${LOCAL_CDS_DATA_DIR}/${MIPS_DIR}/${SGAL4_MIPS}
    if [ -d $d ]; then
        $AWSCP $d s3://${S3_BUCKET}/${S3_DATA_VERSION}/metadata/by_line --recursive
    else
        echo "uploadSGal4MIPs: $d not found"
    fi
}

uploadEMMIPs() {
    local d=${LOCAL_CDS_DATA_DIR}/${MIPS_DIR}/${EM_MIPS}
    if [ -d $d ]; then
        $AWSCP $d s3://${S3_BUCKET}/${S3_DATA_VERSION}/metadata/by_body --recursive
    else
        echo "uploadEMMIPs: $d not found"
    fi
}

uploadEMMatches() {
    $AWSCP \
        ${LOCAL_CDS_DATA_DIR}/${CDS_RESULTS_DIR}/${PER_EM_DIR} \
        s3://${S3_BUCKET}/${S3_DATA_VERSION}/metadata/cdsresults --recursive

}

uploadLMMatches() {
    $AWSCP \
        ${LOCAL_CDS_DATA_DIR}/${CDS_RESULTS_DIR}/${PER_LM_DIR} \
        s3://${S3_BUCKET}/${S3_DATA_VERSION}/metadata/cdsresults --recursive
}

uploadPPP() {
    # upload PPP results
    $AWSCP \
        ${LOCAL_PPP_DATA_DIR}/${PPP_RESULTS_DIR} \
        s3://${S3_BUCKET}/${S3_DATA_VERSION}/metadata/pppresults --recursive
}

uploadDataNotes() {
    # upload data notes
    $AWSCP \
        ${LOCAL_VERSION_DATA_DIR}/DATA_NOTES.md \
        s3://${S3_BUCKET}/${S3_DATA_VERSION}/DATA_NOTES.md
}

uploadConfig() {
    # upload paths.json
    $AWSCP \
        ${LOCAL_VERSION_DATA_DIR}/config.json \
        s3://${S3_BUCKET}/${S3_DATA_VERSION}/config.json
}

uploadSchemas() {
    $AWSCP \
        ${LOCAL_VERSION_DATA_DIR}/schemas \
        s3://${S3_BUCKET}/${S3_DATA_VERSION}/schemas --content-type application/json --recursive
}

uploadCurrentVersion() {
    $AWSCP \
        ${LOCAL_VERSION_DATA_DIR}/current.txt \
        s3://${S3_BUCKET}/current.txt
}

uploadEMMIPs
uploadMCFOMIPs
uploadSGal4MIPs
uploadEMMatches
uploadLMMatches
uploadPPP
uploadDataNotes
uploadConfig
uploadSchemas
uploadCurrentVersion
