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

    private static LoadingCache<MIPInfo, MIPImage> mipsImagesCache;

    static void initializeCache(long maxSize, long expirationInMinutes) {
        LOG.info("Initialize cache: size={} and expiration={}min", maxSize, expirationInMinutes);
        mipsImagesCache = CacheBuilder.newBuilder()
                .concurrencyLevel(16)
                .maximumSize(maxSize)
                .expireAfterAccess(Duration.ofMinutes(expirationInMinutes))
                .build(new CacheLoader<MIPInfo, MIPImage>() {
                    @Override
                    public MIPImage load(MIPInfo mipInfo) {
                        return MIPsUtils.loadMIP(mipInfo);
                    }
                });
    }

    static MIPImage loadMIP(MIPInfo mipInfo) {
        try {
            if (mipInfo == null || !mipInfo.exists()) {
                return null;
            } else {
                return mipsImagesCache == null ? MIPsUtils.loadMIP(mipInfo) : mipsImagesCache.get(mipInfo);
            }
        } catch (ExecutionException e) {
            LOG.error("Error loading {}", mipInfo, e);
            throw new IllegalStateException(e);
        }
    }

}
