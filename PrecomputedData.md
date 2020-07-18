### Data for Pre-computed color depth search results
There are two data types needed for viewing pre-compute color depth search results:
- Color depth MIPs grouped by the LM line name or by EM body ID
- Color depth search results grouped by the MIP identifier

Both the color depth MIPs and the color depth search results are formatted using JSON.

#### LM MIP
LM MIPs are grouped by line in a json file with the same name as the line. 
Below there's an example of just one MIP for line "R10A07" from file `R10A07.json`:  
```json
{
  "results" : [ {
    "id" : "2711777284640997387",
    "publishedName" : "R10A07",
    "libraryName" : "FlyLight Gen1 MCFO",
    "imageURL" : "https://s3.amazonaws.com/janelia-flylight-color-depth/JRC2018_Unisex_20x_HR/FlyLight_Gen1_MCFO/R10A07-20181121_61_G1-GAL4-f-40
x-brain-JRC2018_Unisex_20x_HR-CDM_1.png",
    "thumbnailURL" : "https://s3.amazonaws.com/janelia-flylight-color-depth-thumbnails/JRC2018_Unisex_20x_HR/FlyLight_Gen1_MCFO/R10A07-20181121_
61_G1-GAL4-f-40x-brain-JRC2018_Unisex_20x_HR-CDM_1.jpg",
    "slideCode" : "20181121_61_G1",
    "gender" : "f",
    "mountingProtocol" : "DPX PBS Mounting",
    "anatomicalArea" : "Brain",
    "alignmentSpace" : "JRC2018_Unisex_20x_HR",
    "objective" : "40x",
    "channel" : "1"
  } ]
}
```
The attributes section contains the LM sample attributes that need to be displayed to the user. Also typically
there are multiple samples per LM line and each sample can have 1 or 3 images depending on the resolution, so
file R10A07.json will contain multiple MIP entries.

#### EM MIP
EM MIPs are grouped by body (skeleton) id in a file named using the body id.
The example below is the MIP for body "296358430" from file `296358430.json`
```json
{
  "results" : [ {
    "id" : "2757945469383475211",
    "publishedName" : "296358430",
    "libraryName" : "FlyEM Hemibrain",
    "imageURL" : "https://s3.amazonaws.com/janelia-flylight-color-depth/JRC2018_Unisex_20x_HR/FlyEM_Hemibrain_v1.0/296358430-L-JRC2018_Unisex_2\
0x_HR-CDM.png",
    "thumbnailURL" : "https://s3.amazonaws.com/janelia-flylight-color-depth-thumbnails/JRC2018_Unisex_20x_HR/FlyEM_Hemibrain_v1.0/296358430-L-J\
RC2018_Unisex_20x_HR-CDM.jpg",
    "gender":  "f"
  } ]
}
```
In this case the attributes section is much smaller because the workstation 
does not hold a lot of information about the EM data and typically there won't be 
more than one MIP per skeleton - there might be situations when we have two MIPs
per body ID when we have both the machine segmented neuron and the human traced neuron
but in the future most likely will have only one.

For calculating color depth search results we take all LM MIPs and compare them
with all EM MIPs. So for the example above we all MIPs of line R10A07 and compare
them with MIP#2757945469383475211.
In order to optimize for bidirectional search we write the results in two different files:
one for the EM MIP - `2757945469383475211.json` and one for the LM MIP - `2711777284640997387.json`

The EM result file looks like:
```json
{
  "maskId" : "2757945469383475211",
  "maskPublishedName" : "296358430",
  "maskLibraryName" : "FlyEM Hemibrain",
  "results" : [ {
    "id" : "2711777284640997387",
    "publishedName" : "R10A07",
    "libraryName" : "FlyLight Gen1 MCFO",
    "imageURL" : "https://s3.amazonaws.com/janelia-flylight-color-depth/JRC2018_Unisex_20x_HR/FlyLight_Gen1_MCFO_gamma_corrected/R42B08-20190205_62_H
5-GAL4-f-40x-brain-JRC2018_Unisex_20x_HR-CDM_3.png",
    "thumbnailURL" : "https://s3.amazonaws.com/janelia-flylight-color-depth-thumbnails/JRC2018_Unisex_20x_HR/FlyLight_Gen1_MCFO_gamma_corrected/R42B0
8-20190205_62_H5-GAL4-f-40x-brain-JRC2018_Unisex_20x_HR-CDM_3.jpg",
    "slideCode" : "20181121_61_G1",
    "objective" : "40x",
    "gender" : "f",
    "anatomicalArea" : "Brain",
    "alignmentSpace" : "JRC2018_Unisex_20x_HR",
    "channel" : "1",
    "mountingProtocol" : "DPX PBS Mounting",
    "matchingPixels" : 190,
    "matchingRatio" : 0.0490702479338843,
    "gradientAreaGap" : 2438,
    "normalizedGapScore" : 998.3483729833197,
    "normalizedScore" : 998.3483729833197
  } ]
}
```
and the corresponding LM result:
```json
{
  "maskId" : "2711777284640997387",
  "maskPublishedName" : "R10A07",
  "maskLibraryName" : "FlyLight Gen1 MCFO",
  "results" : [ {
    "id" : "2757945469383475211",
    "publishedName" : "296358430",
    "normalizedScore" : 998.3483729833197,
    "imageURL" : "https://s3.amazonaws.com/janelia-flylight-color-depth/JRC2018_Unisex_20x_HR/FlyEM_Hemibrain_v1.0/296358430-L-JRC2018_Unisex_2\
0x_HR-CDM.png",
    "thumbnailURL" : "https://s3.amazonaws.com/janelia-flylight-color-depth-thumbnails/JRC2018_Unisex_20x_HR/FlyEM_Hemibrain_v1.0/296358430-L-J\
RC2018_Unisex_20x_HR-CDM.jpg",
    "library" : "FlyEM Hemibrain",
    "matchingPixels" : "190",
    "matchingRatio" : "0.0490702479338843",
    "gradientAreaGap" : "2438",
    "normalizedGapScore" : "998.3483729833197"
  } ]
}
```
For display purposes the results always contain the attributes and the image 
of the matched MIP.
