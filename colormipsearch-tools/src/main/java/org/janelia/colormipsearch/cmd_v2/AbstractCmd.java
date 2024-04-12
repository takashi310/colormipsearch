package org.janelia.colormipsearch.cmd_v2;

import org.apache.commons.lang3.StringUtils;

@Deprecated
abstract class AbstractCmd {
    static final long _1M = 1024 * 1024;

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
