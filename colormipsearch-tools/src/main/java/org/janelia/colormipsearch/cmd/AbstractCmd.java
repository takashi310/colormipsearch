package org.janelia.colormipsearch.cmd;

import org.apache.commons.lang3.StringUtils;
import org.janelia.colormipsearch.config.Config;
import org.janelia.colormipsearch.config.ConfigProvider;
import org.janelia.colormipsearch.dao.DaosProvider;

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

    Config getConfig() {
        return ConfigProvider.getInstance()
                .fromDefaultResources()
                .fromFile(getArgs().getConfigFileName())
                .get();
    }

    DaosProvider getDaosProvider() {
        return DaosProvider.getInstance(getConfig());
    }
}
