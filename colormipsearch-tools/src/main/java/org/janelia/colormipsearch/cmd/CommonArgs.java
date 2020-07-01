package org.janelia.colormipsearch.cmd;

import com.beust.jcommander.Parameter;

class CommonArgs {
    @Parameter(names = {"--outputDir", "-od"}, description = "Output directory")
    String outputDir;

    @Parameter(names = "--no-pretty-print", description = "Do not pretty print the results", arity = 0)
    boolean noPrettyPrint = false;

    @Parameter(names = {"--help", "-h"}, description = "Display the help message", help = true, arity = 0)
    boolean displayHelpMessage = false;
}
