### Release Version: 2.1.0

### Release Date: 2020-09-15

### Notes
* Data format change - removed the attributes map
* Precomputed matches between Flylight Split GAL4 and MCFO vs Hemibrain 1.1
* Color Depth Search Paramers were the same as the ones for 1.1.0:
```json
{
  "defaultMaskThreshold" : "20",
  "dataThreshold" : "20",
  "xyShift" : "2",
  "mirrorMask" : "true",
  "pixColorFluctuation" : "1.0",
  "pctPositivePixels" : "1.0"
}
```

### Release Version: 1.1.0

### Release Date: 2020-04-15

### Notes
* Precomputed matches between Flylight Split GAL4 and MCFO vs Hemibrain 1.0.1
* Created the segmentation for Flylight Split GAL4 and MCFO libraries and
the color depth search between hemibrain neurons and flylight was actually run
using the segmented MIPs
* For Hemibrain neurons which cross the middle section also
generated the flipped neuron by mirroring it and adding the original
and also compared this against all flylight MIPs.
* Color Depth Search Paramers:
```json
{
  "defaultMaskThreshold" : "20",
  "dataThreshold" : "20",
  "xyShift" : "2",
  "mirrorMask" : "true",
  "pixColorFluctuation" : "1.0",
  "pctPositivePixels" : "1.0"
}
```
* For calculating the negative scores - gradient area gap 
and the area of regions with high expression - we selected the MIPs for the top 300 lines
and we used precomnputed RGB masks generated with a radius of 20px
