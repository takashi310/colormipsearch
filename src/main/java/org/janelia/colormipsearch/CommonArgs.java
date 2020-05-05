package org.janelia.colormipsearch;

import com.beust.jcommander.Parameter;

class CommonArgs {
    @Parameter(names = {"--outputDir", "-od"}, description = "Output directory")
    String outputDir;

    @Parameter(names = "-h", description = "Display the help message", help = true, arity = 0)
    boolean displayHelpMessage = false;
}
