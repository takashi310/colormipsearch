package org.janelia.colormipsearch.utils;

import java.time.Duration;
import java.util.concurrent.ExecutionException;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import org.janelia.colormipsearch.api.cdmips.MIPImage;
import org.janelia.colormipsearch.api.cdmips.MIPMetadata;
import org.janelia.colormipsearch.api.cdmips.MIPsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CachedMIPsUtils {
    private static final Logger LOG = LoggerFactory.getLogger(CachedMIPsUtils.class);

    private static LoadingCache<MIPMetadata, MIPImage> mipsImagesCache;

    public static void initializeCache(long maxSize, long expirationInSeconds) {
        if (maxSize > 0) {
            LOG.info("Initialize cache: size={} and expiration={}s", maxSize, expirationInSeconds);
            CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder()
                    .concurrencyLevel(8)
                    .maximumSize(maxSize);
            if (expirationInSeconds > 0) {
                cacheBuilder.expireAfterAccess(Duration.ofSeconds(expirationInSeconds));
            }
            mipsImagesCache = cacheBuilder
                    .build(new CacheLoader<MIPMetadata, MIPImage>() {
                        @Override
                        public MIPImage load(MIPMetadata mipInfo) {
                            return MIPsUtils.loadMIP(mipInfo);
                        }
                    });
        } else {
            mipsImagesCache = null;
        }
    }

    public static MIPImage loadMIP(MIPMetadata mipInfo) {
        try {
            if (mipInfo == null || !MIPsUtils.exists(mipInfo)) {
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
