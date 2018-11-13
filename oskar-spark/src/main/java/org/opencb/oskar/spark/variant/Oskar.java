package org.opencb.oskar.spark.variant;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
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
import org.opencb.oskar.spark.variant.analysis.VariantSetStatsTransformer;
import org.opencb.oskar.spark.variant.analysis.VariantStatsTransformer;
import org.opencb.oskar.spark.variant.udf.VariantUdfManager;
import scala.collection.Iterator;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.apache.spark.sql.functions.col;

/**
 * Created on 27/09/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class Oskar {

    private final SparkSession spark;

    public Oskar() {
        this.spark = null;
    }

    public Oskar(SparkSession spark) {
        this.spark = spark;
        if (spark != null) {
            new VariantUdfManager().loadVariantUdfs(spark);
        }
    }

    public Dataset<Row> load(Path path) throws OskarException {
        return load(path.toAbsolutePath().toString());
    }

    public Dataset<Row> load(String path) throws OskarException {
        Dataset<Row> dataset;
        if (path.endsWith("avro") || path.endsWith("avro.gz")) {
            // Do not fail if the file extension is "avro.gz"
            spark.sparkContext().hadoopConfiguration().set("avro.mapred.ignore.inputs.without.extension", "false");

            dataset = spark.read().format("com.databricks.spark.avro").load(path);
        } else if (path.endsWith("parquet")) {
            dataset = spark.read().format("parquet").load(path);
        } else {
            throw OskarException.unsupportedFileFormat(path);
        }

        // Read and add metadata
        String metadataPath = path + ".meta.json.gz";

        if (Paths.get(metadataPath).toFile().exists()) {
            dataset = addVariantMetadata(dataset, metadataPath);
        }

        return dataset;
    }

    public VariantMetadata loadMetadata(String path) throws OskarException {
        ObjectMapper objectMapper = new ObjectMapper();
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
     * @param metadataPath VariantMetadata to add
     * @return  Modified dataset
     * @throws OskarException if there is an error reading the metadata file
     */
    public Dataset<Row> addVariantMetadata(Dataset<Row> dataset, String metadataPath) throws OskarException {
        VariantMetadata variantMetadata = loadMetadata(metadataPath);
        dataset = addVariantMetadata(dataset, variantMetadata);
        return dataset;
    }

    /**
     * Writes the VariantMetadata into the schema metadata from the given dataset.
     *
     * @param dataset Dataset to modify
     * @param variantMetadata VariantMetadata to add
     * @return  Modified dataset
     */
    public Dataset<Row> addVariantMetadata(Dataset<Row> dataset, VariantMetadata variantMetadata) {
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

                    Metadata attrMetadata = new MetadataBuilder()
                            .putString("phenotype", phenotype)
                            .putString("sex", sex)
                            .putString("mother", mother)
                            .putString("father", father)
                            .build();

                    pedigreeMap.get(study.getId()).get(family).put(sample.getId(), attrMetadata);
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

        return new MetadataBuilder()
                .putMetadata("samples", samplesMetadata.build())
                .putMetadata("pedigree", pedigreeMetadata.build())
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

    public List<String> samples(Dataset<Row> df, String studyId) throws OskarException {
        Metadata samplesMetadata = getSamplesMetadata(df);

        String[] sampleNames = samplesMetadata.getStringArray(studyId);
        if (sampleNames == null) {
            throw OskarException.unknownStudy(studyId, scala.collection.JavaConversions.mapAsJavaMap(samplesMetadata.map()).keySet());
        }
        return Arrays.asList(sampleNames);
    }

    private Metadata getSamplesMetadata(Dataset<Row> df) {
        return ((StructType) ((ArrayType) df.schema().apply("studies").dataType()).elementType()).apply("samplesData")
                .metadata().getMetadata("samples");
    }

    // Pedigree management
    public Map<String, List<Pedigree>> pedigree(Dataset<Row> df) {
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

                // Map to contain all member of a pedigree/family, and siblings
                Map<String, Member> membersMap = new HashMap<>();
                Map<String, List<String>> siblingsMap = new HashMap<>();

                // sample metadata = Map<sample ID, attribute metadata>
                Metadata sampleMetadata = familyMetadata.getMetadata(familyName);
                Iterator<String> sampleIt = sampleMetadata.map().keysIterator();
                while (sampleIt.hasNext()) {
                    String sampleId = sampleIt.next();
                    if (!membersMap.containsKey(sampleId)) {
                        membersMap.put(sampleId, new Member().setId(sampleId));
                    }
                    Member member = membersMap.get(sampleId);

                    Metadata attrMetadata = sampleMetadata.getMetadata(sampleId);
                    String fatherId = null;
                    String motherId = null;
                    // Father
                    if (attrMetadata.contains("father")) {
                        fatherId = attrMetadata.getString("father");
                        if (!membersMap.containsKey(fatherId)) {
                            Member father = new Member().setId(fatherId);
                            membersMap.put(fatherId, father);
                        }
                        member.setFather(membersMap.get(fatherId));
                    }

                    // Mother
                    if (attrMetadata.contains("mother")) {
                        motherId = attrMetadata.getString("mother");
                        if (!membersMap.containsKey(motherId)) {
                            Member mother = new Member().setId(motherId);
                            membersMap.put(motherId, mother);
                        }
                        member.setMother(membersMap.get(motherId));
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

                // And, add pedigree to study
                pedigreeMap.get(studyId).add(pedigree);
            }
        }
        return pedigreeMap;
    }

    public List<Pedigree> pedigree(Dataset<Row> df, String studyId) throws OskarException {
        Map<String, List<Pedigree>> pedigreeMap = pedigree(df);
        if (!pedigreeMap.containsKey(studyId)) {
            throw OskarException.unknownStudy(studyId, pedigreeMap.keySet());
        }

        return pedigree(df).get(studyId);
    }

    private Metadata getPedigreeMetadata(Dataset<Row> df) {
        return ((StructType) ((ArrayType) df.schema().apply("studies").dataType()).elementType()).apply("samplesData")
                .metadata().getMetadata("pedigree");
    }

    public Dataset<Row> stats(Dataset<Row> df, String studyId, String cohort, List<String> samples) {
        return new VariantStatsTransformer(studyId, cohort, samples).transform(df);
    }

    public Dataset<Row> globalStats(Dataset<Row> df, String studyId, String fileId) {
        return new VariantSetStatsTransformer(studyId, fileId).transform(df);
    }

}
