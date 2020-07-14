package org.janelia.colormipsearch.cmd;

import org.apache.commons.lang3.StringUtils;

abstract class AbstractCmd {

    private final String commandName;

    AbstractCmd(String commandName) {
        this.commandName = commandName;
    }

    public String getCommandName() {
        return commandName;
    }

    abstract AbstractCmdArgs getArgs();

    boolean matches(String commandName) {
        return StringUtils.isNotBlank(commandName) && StringUtils.equals(this.commandName, commandName);
    }

    abstract void execute();
}
