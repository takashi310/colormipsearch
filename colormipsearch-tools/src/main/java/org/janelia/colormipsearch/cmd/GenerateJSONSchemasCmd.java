package org.janelia.colormipsearch.cmd;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.URI;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import com.fasterxml.jackson.module.jsonSchema.JsonSchema;
import com.fasterxml.jackson.module.jsonSchema.JsonSchemaGenerator;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.janelia.colormipsearch.api.Results;
import org.janelia.colormipsearch.api.cdmips.AbstractMetadata;
import org.janelia.colormipsearch.api.cdsearch.CDSMatches;
import org.janelia.colormipsearch.api.pppsearch.EmPPPMatches;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Perform color depth mask search on a Spark cluster.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class GenerateJSONSchemasCmd extends AbstractCmd {

    private static final Logger LOG = LoggerFactory.getLogger(GenerateJSONSchemasCmd.class);

    @Parameters(commandDescription = "Grooup MIPs by published name")
    private static class GenerateJSONSchemasArgs extends AbstractCmdArgs {


        @Parameter(names = {"--schemas-directory", "-sdir"}, description = "Schemas sub-directory")
        String schemasOutput = "schemas";

        @ParametersDelegate
        final CommonArgs commonArgs;

        GenerateJSONSchemasArgs(CommonArgs commonArgs) {
            this.commonArgs = commonArgs;
        }
    }

    private final GenerateJSONSchemasArgs args;
    private final ObjectMapper mapper;

    GenerateJSONSchemasCmd(String commandName, CommonArgs commonArgs) {
        super(commandName);
        args = new GenerateJSONSchemasArgs(commonArgs);
        this.mapper = new ObjectMapper()
                .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Override
    GenerateJSONSchemasArgs getArgs() {
        return args;
    }

    @Override
    void execute() {
        Path outputPath = Paths.get(args.commonArgs.outputDir, args.schemasOutput);
        CmdUtils.createOutputDirs(outputPath);
        try {
            JsonSchemaGenerator jsonSchemaGenerator = new JsonSchemaGenerator(mapper);
            JavaType mipResultstype = mapper.getTypeFactory().constructParametricType(Results.class,
                    mapper.getTypeFactory().constructCollectionType(List.class, ColorDepthMetadata.class));
            JsonSchema mipsSchema = jsonSchemaGenerator.generateSchema(mipResultstype);
            writeSchemaToFile(mipsSchema,
                    CmdUtils.getOutputFile(outputPath, "mips.schema"));

            JsonSchema matchesSchema = jsonSchemaGenerator.generateSchema(CDSMatches.class);
            writeSchemaToFile(matchesSchema,
                    CmdUtils.getOutputFile(outputPath, "cdsMatches.schema"));

            JsonSchema pppsSchema = jsonSchemaGenerator.generateSchema(EmPPPMatches.class);
            writeSchemaToFile(pppsSchema,
                    CmdUtils.getOutputFile(outputPath, "pppMatches.schema"));

        } catch (Exception e) {
            LOG.error("Error generating schema", e);
        }
    }

    private void writeSchemaToFile(JsonSchema schema, File f) throws IOException {
        ObjectWriter writer = args.commonArgs.noPrettyPrint ? mapper.writer() : mapper.writerWithDefaultPrettyPrinter();
        if (f == null) {
            writer.writeValue(System.out, schema);
        } else {
            writer.writeValue(f, schema);
        }
    }
}
