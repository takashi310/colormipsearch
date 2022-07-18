package org.janelia.colormipsearch.cmd;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.github.victools.jsonschema.generator.ConfigFunction;
import com.github.victools.jsonschema.generator.MethodScope;
import com.github.victools.jsonschema.generator.OptionPreset;
import com.github.victools.jsonschema.generator.SchemaGenerator;
import com.github.victools.jsonschema.generator.SchemaGeneratorConfig;
import com.github.victools.jsonschema.generator.SchemaGeneratorConfigBuilder;
import com.github.victools.jsonschema.generator.SchemaVersion;
import com.github.victools.jsonschema.generator.TypeScope;
import com.github.victools.jsonschema.module.jackson.JacksonModule;

import org.janelia.colormipsearch.model.CDMatch;
import org.janelia.colormipsearch.model.EMNeuronMetadata;
import org.janelia.colormipsearch.model.JsonRequired;
import org.janelia.colormipsearch.dataio.fs.FSUtils;
import org.janelia.colormipsearch.model.LMNeuronMetadata;
import org.janelia.colormipsearch.model.PPPMatch;
import org.janelia.colormipsearch.results.ResultMatches;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generate JSON schema command.
 */
public class GenerateJSONSchemasCmd extends AbstractCmd {

    private static final String SCHEMAS_BASE_URI = "https://neuronbridge.janelia.org/schemas/";

    private static final String CDS_RESULTS_SCHEMA = "cdsMatches.schema";
    private static final String PPP_RESULTS_SCHEMA = "pppMatches.schema";

    private static final Logger LOG = LoggerFactory.getLogger(GenerateJSONSchemasCmd.class);

    @Parameters(commandDescription = "Generate JSON schema command args")
    private static class GenerateJSONSchemasArgs extends AbstractCmdArgs {

        @Parameter(names = {"--schemas-directory", "-sdir"}, description = "Schemas sub-directory")
        String schemasOutput = "schemas";

        GenerateJSONSchemasArgs(CommonArgs commonArgs) {
            super(commonArgs);
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
        FSUtils.createDirs(outputPath);
        try {
            JacksonModule module = new JacksonModule();
            SchemaGeneratorConfigBuilder configBuilder = new SchemaGeneratorConfigBuilder(SchemaVersion.DRAFT_2020_12, OptionPreset.PLAIN_JSON)
                    .with(module)
                    ;

            // we only configure the builder for fields and for types because that is what the schema generator needs right now
            // it does not use any method for schema generation

            configBuilder.forTypesInGeneral()
                    .withIdResolver(getListTypeResultsHandler(
                            SCHEMAS_BASE_URI + CDS_RESULTS_SCHEMA + ".json",
                            SCHEMAS_BASE_URI + PPP_RESULTS_SCHEMA + ".json"))
                    .withTitleResolver(getListTypeResultsHandler(
                            "Color Depth Search results",
                            "Patch per Pix search results"))
                    .withDescriptionResolver(getListTypeResultsHandler(
                            "Color Depth Search results",
                            "Patch per Pix search results"))
                    ;

            configBuilder.forFields()
                    .withRequiredCheck(fieldScope -> {
                        // we consider a field to be required if either the field or the getter
                        // is explicitly annotated with JsonProperty
                        MethodScope methodScope = fieldScope.findGetter();
                        JsonRequired fieldAnnotation = fieldScope.getAnnotation(JsonRequired.class);
                        JsonRequired methodAnnotation = methodScope == null ? null : methodScope.getAnnotation(JsonRequired.class);
                        boolean required = fieldAnnotation != null || methodAnnotation != null;
                        return required;
                    })
                    .withStringFormatResolver(fieldScope -> {
                        if (fieldScope.getDeclaredName().toLowerCase().contains("url")) {
                            return "uri";
                        } else {
                            return null;
                        }
                    })
                    ;

            SchemaGeneratorConfig config = configBuilder.build();
            SchemaGenerator generator = new SchemaGenerator(config);

            JsonNode cdsMatchesSchema =
                    generator.generateSchema(new TypeReference<ResultMatches<
                            EMNeuronMetadata,
                            LMNeuronMetadata,
                            CDMatch<EMNeuronMetadata, LMNeuronMetadata>>>(){}.getType());
            writeSchemaToFile(cdsMatchesSchema,
                    FSUtils.getOutputFile(outputPath, CDS_RESULTS_SCHEMA));

            JsonNode pppMatchesSchema =
                    generator.generateSchema(new TypeReference<ResultMatches<
                            EMNeuronMetadata,
                            LMNeuronMetadata,
                            PPPMatch<EMNeuronMetadata, LMNeuronMetadata>>>(){}.getType());
            writeSchemaToFile(pppMatchesSchema,
                    FSUtils.getOutputFile(outputPath, PPP_RESULTS_SCHEMA));

        } catch (Exception e) {
            LOG.error("Error generating schema", e);
        }
    }

    private void writeSchemaToFile(JsonNode schema, File f) throws IOException {
        ObjectWriter writer = args.commonArgs.noPrettyPrint ? mapper.writer() : mapper.writerWithDefaultPrettyPrinter();
        if (f == null) {
            writer.writeValue(System.out, schema);
        } else {
            writer.writeValue(f, schema);
        }
    }

    private ConfigFunction<TypeScope, String> getListTypeResultsHandler(String cdsResultsMessage, String pppResultsMessage) {
        return scope -> {
            if (scope.getType().getErasedType() == ResultMatches.class || scope.getType().isInstanceOf(ResultMatches.class)) {
                if (scope.getTypeParameterFor(ResultMatches.class, 2).getErasedType() == CDMatch.class) {
                    // ResultMatches<EM, LM, Match<EM, LM>>
                    return cdsResultsMessage;
                } else if (scope.getTypeParameterFor(ResultMatches.class, 2).getErasedType() == PPPMatch.class) {
                    // Results<List<PPPMatch>>
                    return pppResultsMessage;
                }
            }
            return null;
        };
    }
}
