package org.janelia.colormipsearch.cmd.dataexport;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.colormipsearch.dto.AbstractNeuronMetadata;
import org.janelia.colormipsearch.model.FileType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * URLTransformer is responsible for "transforming" absolute URLs into relative URLs
 * from a specified path component position.
 * The transformer typically is applied to http URIs, but it can also handle non-http URIs
 * if changeNonHttpURLs is set.
 */
public class URLTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(URLTransformer.class);

    public static class URLTransformerParams {
        final int relativeURLStart;
        final boolean changeNonHttpURLs;

        public URLTransformerParams(int relativeURLStart, boolean changeNonHttpURLs) {
            this.relativeURLStart = relativeURLStart;
            this.changeNonHttpURLs = changeNonHttpURLs;
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this)
                    .append("relativeURLStart", relativeURLStart)
                    .append("changeNonHttpURLs", changeNonHttpURLs)
                    .toString();
        }
    }

    private final URLTransformerParams defaultURLTransformParams;
    private final Map<FileType, URLTransformerParams> urlTransformerParamsMap;

    public URLTransformer(int defaultRelativeURLStart,
                          Map<FileType, URLTransformerParams> urlTransformerParamsMap) {
        this.defaultURLTransformParams = new URLTransformerParams(defaultRelativeURLStart, false);
        this.urlTransformerParamsMap = urlTransformerParamsMap;
    }

    public String relativizeURL(FileType fileType, String aUrl) {
        URLTransformerParams urlTransformerParams = getURLTransformParams(fileType);
        if (StringUtils.isBlank(aUrl)) {
            return "";
        } else {
            if (urlTransformerParams.relativeURLStart < 0) {
                return aUrl;
            } else {
                Path p;
                if (StringUtils.startsWithIgnoreCase(aUrl, "https://") ||
                        StringUtils.startsWithIgnoreCase(aUrl, "http://")) {
                    URI uri = URI.create(aUrl.replace(' ', '+'));
                    p = Paths.get(uri.getPath());
                } else {
                    if (urlTransformerParams.changeNonHttpURLs) {
                        p = Paths.get(aUrl);
                    } else {
                        // if the flag to transform nonHttp is not set
                        // leave the URL as is
                        return aUrl;
                    }
                }
                int nPathComponents = p.getNameCount();
                if (urlTransformerParams.relativeURLStart >= nPathComponents) {
                    LOG.warn("URL {} for {} has fewer components than configured: {} so it will be left as is",
                            aUrl, fileType, urlTransformerParams);
                }
                return p.subpath(urlTransformerParams.relativeURLStart, nPathComponents).toString();
            }
        }
    }

    private URLTransformerParams getURLTransformParams(FileType fileType) {
        if (fileType == null) {
            return defaultURLTransformParams;
        } else {
            return urlTransformerParamsMap.getOrDefault(fileType, defaultURLTransformParams);
        }
    }
}
