package org.opencb.oskar.spark.variant;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.*;
import org.opencb.biodata.models.clinical.pedigree.Member;
import org.opencb.biodata.models.clinical.pedigree.Pedigree;
import org.opencb.biodata.models.commons.Phenotype;
import org.opencb.biodata.models.metadata.Individual;
import org.opencb.biodata.models.metadata.Sample;
import org.opencb.biodata.models.pedigree.Multiples;
import org.opencb.biodata.models.variant.metadata.VariantMetadata;
import org.opencb.biodata.models.variant.metadata.VariantStudyMetadata;
import org.opencb.commons.utils.FileUtils;
import org.opencb.oskar.spark.commons.OskarException;
import org.opencb.oskar.spark.commons.converters.DataTypeUtils;
import scala.collection.Iterator;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;

/**
 * Created on 13/11/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantMetadataManager {

    private final ObjectMapper objectMapper;

    public VariantMetadataManager() {
        objectMapper = new ObjectMapper()
                .configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true)
                .configure(SerializationFeature.WRITE_NULL_MAP_VALUES, false)
                .setSerializationInclusion(JsonInclude.Include.NON_DEFAULT);
    }

    public String getMetadataPath(String path) throws OskarException {
        if (Paths.get(path + ".meta.json.gz").toFile().exists()) {
            return path + ".meta.json.gz";
        } else if (Paths.get(path + ".meta.json").toFile().exists()) {
            return path + ".meta.json";
        }
        throw OskarException.errorLoadingVariantMetadataFile(new IOException("Variant metadata not found"), path);
    }

    public VariantMetadata readMetadata(String path) throws OskarException {
        try (InputStream is = FileUtils.newInputStream(Paths.get(path))) {
            return objectMapper.readValue(is, VariantMetadata.class);
        } catch (IOException e) {
            throw OskarException.errorLoadingVariantMetadataFile(e, path);
        }
    }

    /**
     * Writes the VariantMetadata into the schema metadata from the given dataset.
     *
     * @param dataset Dataset to modify
     * @param metadataPath VariantMetadata to set
     * @return  Modified dataset
     * @throws OskarException if there is an error reading the metadata file
     */
    protected Dataset<Row> setVariantMetadata(Dataset<Row> dataset, String metadataPath) throws OskarException {
        VariantMetadata variantMetadata = readMetadata(metadataPath);
        dataset = setVariantMetadata(dataset, variantMetadata);
        return dataset;
    }

    /**
     * Writes the VariantMetadata into the schema metadata from the given dataset.
     *
     * @param dataset Dataset to modify
     * @param variantMetadata VariantMetadata to set
     * @return  Modified dataset
     */
    public Dataset<Row> setVariantMetadata(Dataset<Row> dataset, VariantMetadata variantMetadata) {
        Metadata metadata = createDatasetMetadata(variantMetadata);

        ArrayType studiesArrayType = (ArrayType) dataset.schema().apply("studies").dataType();
        StructType studyStructType = ((StructType) studiesArrayType.elementType());

        // Add metadata to samplesData field
        StructField samplesDataSchemaWithMetadata = DataTypeUtils.addMetadata(metadata, studyStructType.apply("samplesData"));

        // Replace samplesData field
        StructType elementType = DataTypeUtils.replaceField(studyStructType, samplesDataSchemaWithMetadata);

        return dataset.withColumn("studies", col("studies").as("studies", metadata))
                .withColumn("studies", col("studies").cast(new ArrayType(elementType, studiesArrayType.containsNull())));
    }

    public VariantMetadata variantMetadata(Dataset<Row> df) {
        Metadata variantMetadata = getMetadata(df).getMetadata("variantMetadata");

        try {
            return objectMapper.readValue(variantMetadata.toString(), VariantMetadata.class);
        } catch (IOException e) {
            throw OskarException.errorReadingVariantMetadataFromDataframe(e);
        }
    }

    private Metadata createDatasetMetadata(VariantMetadata variantMetadata) {
        Map<String, List<String>> samplesMap = new HashMap<>();
        Map<String, Map<String, Map<String, Metadata>>> pedigreeMap = new HashMap<>();
        for (VariantStudyMetadata study: variantMetadata.getStudies()) {
            if (!pedigreeMap.containsKey(study.getId())) {
                pedigreeMap.put(study.getId(), new HashMap<>());
            }
            List<String> samples = new ArrayList<>();

            for (Individual individual : study.getIndividuals()) {
                String family = individual.getFamily();
                if (!pedigreeMap.get(study.getId()).containsKey(family)) {
                    pedigreeMap.get(study.getId()).put(family, new HashMap<>());
                }

                String phenotype = individual.getPhenotype() == null ? "" : individual.getPhenotype();
                String sex = individual.getSex() == null ? "" : individual.getSex();
                String mother = individual.getMother() == null ? "" : individual.getMother();
                String father = individual.getFather() == null ? "" : individual.getFather();

                for (Sample sample : individual.getSamples()) {
                    samples.add(sample.getId());

                    MetadataBuilder metadataBuilder = new MetadataBuilder()
                            .putString("phenotype", phenotype)
                            .putString("sex", sex)
                            .putString("mother", mother)
                            .putString("father", father);

                    if (MapUtils.isNotEmpty(sample.getAnnotations())) {
                        for (Map.Entry<String, String> entry : sample.getAnnotations().entrySet()) {
                            metadataBuilder.putString(entry.getKey(), entry.getValue());
                        }
                    }

                    pedigreeMap.get(study.getId()).get(family).put(sample.getId(), metadataBuilder.build());
                }
            }
            samplesMap.put(study.getId(), samples);
        }

        // Sample management
        MetadataBuilder samplesMetadata = new MetadataBuilder();
        for (Map.Entry<String, List<String>> entry : samplesMap.entrySet()) {
            samplesMetadata.putStringArray(entry.getKey(), entry.getValue().toArray(new String[0]));
        }

        // Pedigree management
        MetadataBuilder pedigreeMetadata = new MetadataBuilder();
        for (String studyId: pedigreeMap.keySet()) {
            MetadataBuilder familyMetadata = new MetadataBuilder();
            for (String familyId: pedigreeMap.get(studyId).keySet()) {
                MetadataBuilder sampleIdMetadata = new MetadataBuilder();
                for (String sampleId: pedigreeMap.get(studyId).get(familyId).keySet()) {
                    sampleIdMetadata.putMetadata(sampleId, pedigreeMap.get(studyId).get(familyId).get(sampleId));
                }
                familyMetadata.putMetadata(familyId, sampleIdMetadata.build());
            }
            pedigreeMetadata.putMetadata(studyId, familyMetadata.build());
        }

        Metadata metadata;
        try {
            metadata = Metadata.fromJson(objectMapper.writeValueAsString(variantMetadata));
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }

        return new MetadataBuilder()
                .putMetadata("samples", samplesMetadata.build())
                .putMetadata("pedigrees", pedigreeMetadata.build())
                .putMetadata("variantMetadata", metadata)
                .build();
    }

    // Sample names management
    public Map<String, List<String>> samples(Dataset<Row> df) {
        Metadata samplesMetadata = getSamplesMetadata(df);

        Map<String, List<String>> map = new HashMap<>();
        Iterator<String> it = samplesMetadata.map().keysIterator();
        while (it.hasNext()) {
            String studyId = it.next();
            String[] sampleNames = samplesMetadata.getStringArray(studyId);
            map.put(studyId, Arrays.asList(sampleNames));
        }

        return map;
    }

    public List<String> samples(Dataset<Row> df, String studyId) {
        Metadata samplesMetadata = getSamplesMetadata(df);

        String[] sampleNames = samplesMetadata.getStringArray(studyId);
        if (sampleNames == null) {
            throw OskarException.unknownStudy(studyId, scala.collection.JavaConversions.mapAsJavaMap(samplesMetadata.map()).keySet());
        }
        return Arrays.asList(sampleNames);
    }

    // Pedigree management
    public Map<String, List<Pedigree>> pedigrees(Dataset<Row> df) {
        // Pedigree metadata = Map<study ID, family metadata>
        Metadata pedigreeMetadata = getPedigreeMetadata(df);

        Map<String, List<Pedigree>> pedigreeMap = new HashMap<>();
        Iterator<String> studyIt = pedigreeMetadata.map().keysIterator();
        while (studyIt.hasNext()) {
            String studyId = studyIt.next();
            pedigreeMap.put(studyId, new ArrayList<>());

            // Family metadata = Map<family name, sample metadata>
            Metadata familyMetadata = pedigreeMetadata.getMetadata(studyId);
            Iterator<String> familyIt = familyMetadata.map().keysIterator();
            while (familyIt.hasNext()) {
                String familyName = familyIt.next();
                Pedigree pedigree = new Pedigree(familyName, new ArrayList<>(), new HashMap<>());

                // Map to contain all member of a pedigrees/family, and siblings
                Map<String, Member> membersMap = new HashMap<>();
                Map<String, List<String>> siblingsMap = new HashMap<>();

                // sample metadata = Map<sample ID, attribute metadata>
                Metadata sampleMetadata = familyMetadata.getMetadata(familyName);
                Iterator<String> sampleIt = sampleMetadata.map().keysIterator();
                while (sampleIt.hasNext()) {
                    String sampleId = sampleIt.next();
                    if (!membersMap.containsKey(sampleId)) {
                        membersMap.put(sampleId, new Member().setId(sampleId).setAttributes(new HashMap<>()));
                    }
                    Member member = membersMap.get(sampleId);

                    Metadata attrMetadata = sampleMetadata.getMetadata(sampleId);
                    String fatherId = null;
                    String motherId = null;
                    // Father
                    if (attrMetadata.contains("father")) {
                        fatherId = attrMetadata.getString("father");
                        if (StringUtils.isNotEmpty(fatherId)) {
                            Member father = membersMap.computeIfAbsent(fatherId, id -> new Member().setId(id));
                            member.setFather(father);
                        }
                    }

                    // Mother
                    if (attrMetadata.contains("mother")) {
                        motherId = attrMetadata.getString("mother");
                        if (StringUtils.isNotEmpty(motherId)) {
                            Member mother = membersMap.computeIfAbsent(motherId, id -> new Member().setId(id));
                            member.setFather(mother);
                        }
                    }

                    if (StringUtils.isNotEmpty(fatherId) && StringUtils.isNotEmpty(motherId)) {
                        String id = fatherId + "_" + motherId;
                        if (!siblingsMap.containsKey(id)) {
                            siblingsMap.put(id, new ArrayList<>());
                        }
                        siblingsMap.get(id).add(sampleId);
                    }

                    // Other attributes
                    member.setSex(Member.Sex.getEnum(attrMetadata.getString("sex")));
                    member.setPhenotypes(Collections.singletonList(new Phenotype(attrMetadata.getString("phenotype"),
                            attrMetadata.getString("phenotype"), null)));

                    Iterator<String> iterator = attrMetadata.map().keys().iterator();
                    while (iterator.hasNext()) {
                        String key = iterator.next();
                        switch (key) {
                            case "mother":
                            case "father":
                            case "sex":
                            case "phenotype":
                                break;
                            default:
                                if (StringUtils.isNotEmpty(attrMetadata.getString(key))) {
                                    member.getAttributes().put(key, attrMetadata.getString(key));
                                }
                                break;
                        }
                    }

                    // Finally, add member to the pedigreee/family
                    pedigree.getMembers().add(member);
                }

                // Update siblings
                for (Map.Entry<String, Member> entry: membersMap.entrySet()) {
                    if (entry.getValue().getFather() != null & entry.getValue().getMother() != null
                            && entry.getValue().getFather().getId() != null & entry.getValue().getMother().getId() != null) {
                        String id = entry.getValue().getFather().getId() + "_" + entry.getValue().getMother().getId();
                        if (siblingsMap.containsKey(id)) {
                            Multiples multiples = new Multiples(null, new ArrayList<>());
                            for (String siblingId: siblingsMap.get(id)) {
                                if (!siblingId.equals(entry.getKey())) {
                                    multiples.getSiblings().add(siblingId);
                                }
                            }
                            membersMap.get(entry.getKey()).setMultiples(multiples);
                        }
                    }
                }

                // And, add pedigrees to study
                pedigreeMap.get(studyId).add(pedigree);
            }
        }
        return pedigreeMap;
    }

    public List<Pedigree> pedigrees(Dataset<Row> df, String studyId) {
        Map<String, List<Pedigree>> pedigreesMap = pedigrees(df);
        if (!pedigreesMap.containsKey(studyId)) {
            throw OskarException.unknownStudy(studyId, pedigreesMap.keySet());
        }
        return pedigreesMap.get(studyId);
    }

    public Pedigree pedigree(Dataset<Row> df, String studyId, String family) {
        List<Pedigree> pedigrees = pedigrees(df, studyId);
        for (Pedigree pedigree : pedigrees) {
            if (pedigree.getName().equals(family)) {
                return pedigree;
            }
        }
        throw OskarException.unknownFamily(studyId, family, pedigrees.stream().map(Pedigree::getName).collect(Collectors.toList()));
    }

    private Metadata getMetadata(Dataset<Row> df) {
        ArrayType studies = (ArrayType) df.schema().apply("studies").dataType();
        StructType studyEntry = (StructType) studies.elementType();
        return studyEntry.apply("samplesData").metadata();
    }

    private Metadata getSamplesMetadata(Dataset<Row> df) {
        return getMetadata(df).getMetadata("samples");
    }

    private Metadata getPedigreeMetadata(Dataset<Row> df) {
        return getMetadata(df).getMetadata("pedigrees");
    }


}
