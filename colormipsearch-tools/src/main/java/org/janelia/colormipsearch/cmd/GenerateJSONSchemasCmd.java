package org.janelia.colormipsearch.cmd;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
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

import org.janelia.colormipsearch.api.JsonRequired;
import org.janelia.colormipsearch.api.Results;
import org.janelia.colormipsearch.api.cdsearch.CDSMatches;
import org.janelia.colormipsearch.api.cdsearch.ColorMIPSearchMatchMetadata;
import org.janelia.colormipsearch.api.pppsearch.EmPPPMatch;
import org.janelia.colormipsearch.api.pppsearch.EmPPPMatches;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Perform color depth mask search on a Spark cluster.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class GenerateJSONSchemasCmd extends AbstractCmd {

    private static final String SCHEMAS_BASE_URI = "https://neuronbridge.janelia.org/schemas/";

    private static final String MIPS_SCHEMA = "mips.schema";
    private static final String CDS_RESULTS_SCHEMA = "cdsMatches.schema";
    private static final String PPP_RESULTS_SCHEMA = "pppMatches.schema";

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
            JacksonModule module = new JacksonModule();
            SchemaGeneratorConfigBuilder configBuilder = new SchemaGeneratorConfigBuilder(SchemaVersion.DRAFT_2020_12, OptionPreset.PLAIN_JSON)
                    .with(module)
                    ;

            // we only configure the builder for fields and for types because that is what the schema generator needs right now
            // it does not use any method for schema generation

            configBuilder.forTypesInGeneral()
                    .withIdResolver(getListTypeResultsHandler(
                            SCHEMAS_BASE_URI + MIPS_SCHEMA + ".json",
                            SCHEMAS_BASE_URI + CDS_RESULTS_SCHEMA + ".json",
                            SCHEMAS_BASE_URI + PPP_RESULTS_SCHEMA + ".json"))
                    .withTitleResolver(getListTypeResultsHandler(
                            "Color Depth MIPs for a published EM neuron or LM line",
                            "Color Depth Search results",
                            "Patch per Pix search results"))
                    .withDescriptionResolver(getListTypeResultsHandler(
                            "Color Depth MIPs for a published EM neuron or LM line",
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
                        System.out.println("!!!! FIELD: " + fieldScope.getDeclaringType() + ":"  + fieldScope.getDeclaredName() + "->" + required);
                        return required;
                    })
                    .withStringFormatResolver(fieldScope -> "uri")
                    ;

            SchemaGeneratorConfig config = configBuilder.build();
            SchemaGenerator generator = new SchemaGenerator(config);
            JsonNode mipsSchema = generator.generateSchema(new TypeReference<Results<List<ColorDepthMetadata>>>(){}.getType());
            writeSchemaToFile(mipsSchema,
                    CmdUtils.getOutputFile(outputPath, MIPS_SCHEMA));

            JsonNode matchesSchema = generator.generateSchema(CDSMatches.class);
            writeSchemaToFile(matchesSchema,
                    CmdUtils.getOutputFile(outputPath, CDS_RESULTS_SCHEMA));

            JsonNode pppsSchema = generator.generateSchema(EmPPPMatches.class);
            writeSchemaToFile(pppsSchema,
                    CmdUtils.getOutputFile(outputPath, PPP_RESULTS_SCHEMA));

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

    private ConfigFunction<TypeScope, String> getListTypeResultsHandler(String mipsMessage, String cdsResultsMessage, String pppResultsMessage) {
        return scope -> {
            if (scope.getType().getErasedType() == Results.class || scope.getType().isInstanceOf(Results.class)) {
                if (scope.getTypeParameterFor(Results.class, 0).getTypeParameters().get(0).getErasedType() == ColorDepthMetadata.class) {
                    // Results<List<ColorDepthMetadata>>
                    return mipsMessage;
                } else if (scope.getTypeParameterFor(Results.class, 0).getTypeParameters().get(0).getErasedType() == ColorMIPSearchMatchMetadata.class) {
                    // Results<List<ColorMIPSearchMatchMetadata>>
                    return cdsResultsMessage;
                } else if (scope.getTypeParameterFor(Results.class, 0).getTypeParameters().get(0).getErasedType() == EmPPPMatch.class) {
                    // Results<List<EmPPPMatch>>
                    return pppResultsMessage;
                }
            }
            return null;
        };

    }
}
