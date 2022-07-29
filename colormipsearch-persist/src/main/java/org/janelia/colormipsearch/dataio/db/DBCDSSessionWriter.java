package org.janelia.colormipsearch.dataio.db;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.janelia.colormipsearch.config.Config;
import org.janelia.colormipsearch.dao.DaosProvider;
import org.janelia.colormipsearch.dao.MatchSessionDao;
import org.janelia.colormipsearch.dataio.CDSSessionWriter;
import org.janelia.colormipsearch.dataio.DataSourceParam;
import org.janelia.colormipsearch.model.CDSSessionEntity;

public class DBCDSSessionWriter implements CDSSessionWriter {
    private final MatchSessionDao<CDSSessionEntity> matchSessionDao;

    public DBCDSSessionWriter(Config config) {
        this.matchSessionDao = DaosProvider.getInstance(config).getMatchParametersDao();
    }

    @Override
    public Number createSession(List<DataSourceParam> masksInputs,
                                List<DataSourceParam> targetsInputs,
                                Map<String, Object> params,
                                Set<String> tags) {
        CDSSessionEntity cdsParametersEntity = new CDSSessionEntity();
        cdsParametersEntity.setUsername(System.getProperty("user.name"));
        cdsParametersEntity.setParams(params);
        cdsParametersEntity.addAllTags(tags);
        masksInputs.forEach(ds -> cdsParametersEntity.addMask(ds.toString()));
        targetsInputs.forEach(ds -> cdsParametersEntity.addTarget(ds.toString()));
        matchSessionDao.save(cdsParametersEntity);
        return cdsParametersEntity.getEntityId();
    }
}
