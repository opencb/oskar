package org.opencb.oskar.spark.variant.analysis;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.ml.param.Param;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.ArrayContains;
import org.opencb.biodata.models.clinical.pedigree.Pedigree;
import org.opencb.biodata.models.commons.Disorder;
import org.opencb.biodata.tools.pedigree.ModeOfInheritance;
import org.opencb.oskar.spark.commons.OskarException;
import org.opencb.oskar.spark.variant.VariantMetadataManager;
import org.opencb.oskar.spark.variant.analysis.params.HasPhenotype;
import org.opencb.oskar.spark.variant.analysis.params.HasStudyId;
import org.opencb.oskar.spark.variant.udf.VariantUdfManager;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
/**
 * Filter variants that match a given Mode Of Inheritance pattern.
 *
 * Accepted patterns:
 *  - monoallelic, also known as dominant
 *  - biallelic, also known as recessive
 *  - xLinked
 *  - yLinked
 *
 * Created on 21/11/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class ModeOfInheritanceTransformer extends AbstractTransformer implements HasStudyId, HasPhenotype {

    public static final String MONOALLELIC = "monoallelic"; // dominant
    public static final String BIALLELIC = "biallelic";  // recessive
    public static final String X_LINKED_MONOALLELIC = "xLinkedMonoallelic";
    public static final String X_LINKED_BIALLELIC = "xLinkedBiallelic";
    public static final String Y_LINKED = "yLinked";

    private static final String X_LINKED_MONOALLELIC_INTERNAL = "xlinkedmonoallelic";
    private static final String X_LINKED_BIALLELIC_INTERNAL = "xlinkedbiallelic";
    private static final String Y_LINKED_INTERNAL = "ylinked";

    private final Param<String> familyParam;
    private final Param<String> modeOfInheritanceParam;
    private final Param<Boolean> incompletePenetranceParam;
    private final Param<Boolean> missingAsReferenceParam;

    public ModeOfInheritanceTransformer() {
        this(null);
    }

    public ModeOfInheritanceTransformer(String modeOfInheritance, String family, String phenotype) {
        this(null);
        setModeOfInheritance(modeOfInheritance);
        setFamily(family);
        setPhenotype(phenotype);
    }

    public ModeOfInheritanceTransformer(String uid) {
        super(uid);
        familyParam = new Param<>(this, "family", "Select family to apply the filter");
        modeOfInheritanceParam = new Param<>(this, "modeOfInheritance", "Filter by mode of inheritance from a given family. "
                + "Accepted values: monoallelic (dominant), biallelic (recessive), xLinkedMonoallelic, xLinkedBiallelic, yLinked");
        incompletePenetranceParam = new Param<>(this, "incompletePenetrance",
                "Allow variants with an incomplete penetrance mode of inheritance");
        missingAsReferenceParam = new Param<>(this, "missingAsReference", "");

        setDefault(studyIdParam(), "");
        setDefault(incompletePenetranceParam, false);
        setDefault(missingAsReferenceParam, false);
    }

    public Param<String> familyParam() {
        return familyParam;
    }

    public ModeOfInheritanceTransformer setFamily(String family) {
        set(familyParam, family);
        return this;
    }

    public Param<String> modeOfInheritanceParam() {
        return modeOfInheritanceParam;
    }

    public ModeOfInheritanceTransformer setModeOfInheritance(String modeOfInheritance) {
        set(modeOfInheritanceParam, modeOfInheritance);
        return this;
    }

    public Param<Boolean> incompletePenetranceParam() {
        return incompletePenetranceParam;
    }

    public ModeOfInheritanceTransformer setIncompletePenetrance(boolean incompletePenetrance) {
        set(incompletePenetranceParam, incompletePenetrance);
        return this;
    }

    public Param<Boolean> missingAsReferenceParam() {
        return missingAsReferenceParam;
    }

    public ModeOfInheritanceTransformer setMissingAsReference(boolean missingAsReference) {
        set(missingAsReferenceParam, missingAsReference);
        return this;
    }

    @Override
    public Dataset<Row> transform(Dataset<?> dataset) {
        Dataset<Row> df = (Dataset<Row>) dataset;

        VariantMetadataManager vmm = new VariantMetadataManager();
        String family = getOrDefault(familyParam);
        String moi = getOrDefault(modeOfInheritanceParam);
        String phenotype = getPhenotype();
        String studyId = getStudyId();
        Boolean incompletePenetrance = getOrDefault(incompletePenetranceParam);
        Boolean missingAsReference = getOrDefault(missingAsReferenceParam);

        Map<String, List<String>> samplesMap = vmm.samples(df);
        boolean multiStudy = samplesMap.size() > 1;
        if (StringUtils.isEmpty(studyId)) {
            if (multiStudy) {
                throw OskarException.missingStudy(samplesMap.keySet());
            }
            studyId = samplesMap.keySet().iterator().next();
        }

        Pedigree pedigree = vmm.pedigree(df, studyId, family);

        Map<String, List<String>> gtsMap;
        Disorder disorder = new Disorder(phenotype, phenotype, "", "", Collections.emptyList(), new HashMap<>());
        String moiLowerCase = moi.toLowerCase().replace("_", "");
        switch (moiLowerCase) {
            case MONOALLELIC:
            case "dominant":
                gtsMap = ModeOfInheritance.dominant(pedigree, disorder, incompletePenetrance);
                break;
            case BIALLELIC:
            case "recessive":
                gtsMap = ModeOfInheritance.recessive(pedigree, disorder, incompletePenetrance);
                break;
            case X_LINKED_MONOALLELIC_INTERNAL: // Internal values already in lower case
                gtsMap = ModeOfInheritance.xLinked(pedigree, disorder, true);
                df = df.filter(df.col("chromosome").equalTo("X"));
                break;
            case X_LINKED_BIALLELIC_INTERNAL: // Internal values already in lower case
                gtsMap = ModeOfInheritance.xLinked(pedigree, disorder, false);
                df = df.filter(df.col("chromosome").equalTo("X"));
                break;
            case Y_LINKED_INTERNAL: // Internal values already in lower case
                gtsMap = ModeOfInheritance.yLinked(pedigree, disorder);
                df = df.filter(df.col("chromosome").equalTo("Y"));
                break;
            default:
                throw new IllegalArgumentException("Unknown mode of inheritance '" + moi + "'.");
        }

        List<String> samples = vmm.samples(df, studyId);

        for (Map.Entry<String, List<String>> entry : gtsMap.entrySet()) {
            String sample = entry.getKey();
            List<String> gts = entry.getValue();
            int idx = samples.indexOf(sample);

            if (missingAsReference) {
                if (gts.contains("0/0")) {
                    gts.add("./.");
                }
            }

            Column sampleGt;
            if (multiStudy) {
                sampleGt = VariantUdfManager.study("studies", studyId).getField("samplesData").apply(idx).apply(0);
            } else {
                sampleGt = col("studies").apply(0).getField("samplesData").apply(idx).apply(0);
            }

            if (gts.size() == 1) {
                df = df.filter(sampleGt.equalTo(gts.get(0)));
            } else {
                // Do not use "org.apache.spark.sql.functions.array_contains", as it forces the second element to be a literal
                df = df.filter(new Column(new ArrayContains(
                        lit(gts.toArray(new String[0])).expr(),
                        sampleGt.expr())));
            }
        }

        return df;
    }
}
