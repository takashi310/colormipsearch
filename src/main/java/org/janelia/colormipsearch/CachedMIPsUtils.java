package org.janelia.colormipsearch;

import java.time.Duration;
import java.util.concurrent.ExecutionException;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CachedMIPsUtils {
    private static final Logger LOG = LoggerFactory.getLogger(CachedMIPsUtils.class);

    private static final LoadingCache<MIPInfo, MIPImage> MIP_IMAGES_CACHE = CacheBuilder.newBuilder()
            .maximumSize(200000L)
            .expireAfterAccess(Duration.ofMinutes(10))
            .concurrencyLevel(64)
            .build(new CacheLoader<MIPInfo, MIPImage>() {
                @Override
                public MIPImage load(MIPInfo mipInfo) {
                    return MIPsUtils.loadMIP(mipInfo);
                }
            });

    static MIPImage loadMIP(MIPInfo mipInfo) {
        try {
            if (mipInfo == null || !mipInfo.exists()) {
                return null;
            } else {
                return MIP_IMAGES_CACHE.get(mipInfo);
            }
        } catch (ExecutionException e) {
            LOG.error("Error loading {}", mipInfo, e);
            throw new IllegalStateException(e);
        }
    }

}
