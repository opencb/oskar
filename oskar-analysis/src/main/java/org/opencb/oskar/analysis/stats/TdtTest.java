package org.opencb.oskar.analysis.stats;

import org.apache.commons.lang.StringUtils;
import org.opencb.biodata.models.clinical.pedigree.Member;
import org.opencb.biodata.models.clinical.pedigree.Pedigree;
import org.opencb.biodata.models.clinical.pedigree.PedigreeManager;
import org.opencb.biodata.models.commons.Disorder;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.tools.pedigree.MendelianError;
import org.opencb.biodata.tools.pedigree.MendelianError.GenotypeCode;
import org.opencb.commons.datastore.core.ObjectMap;

import java.util.*;

import static org.opencb.biodata.tools.pedigree.MendelianError.getAlternateAlleleCount;

public class TdtTest {

    public TdtTestResult computeTdtTest(List<Pedigree> pedigrees, Map<String, Genotype> genotypes, Disorder phenotype, String chrom) {
        Set<String> fatherMotherDone = new HashSet<>();

        // Transmission counts
        TdtCounters tdtCounters = new TdtCounters();

        // For each family
        for (Pedigree pedigree : pedigrees) {
            PedigreeManager pedigreeManager = new PedigreeManager(pedigree);
            Set<Member> affectedIndividuals = pedigreeManager.getAffectedIndividuals(phenotype);

            for (Member affectedIndividual : affectedIndividuals) {
                // We need father and mother
                if (affectedIndividual.getFather() == null || affectedIndividual.getMother() == null) {
                    continue;
                }

                // Check if these nuclear family has already been processed
                if (fatherMotherDone.contains(affectedIndividual.getFather() + "_" + affectedIndividual.getMother())) {
                    continue;
                }

                // Get genotype for father and mother and, at least one parent has to be heterozygous

                List<String> siblingIds = new ArrayList<>();
                siblingIds.add(affectedIndividual.getId());
                siblingIds.addAll(affectedIndividual.getMultiples().getSiblings());

                // Init tmp counters
                tdtCounters.initCounters();

                // Consider all offspring in nuclear family
                for (String siblingId : siblingIds) {
                    Member individual = pedigreeManager.getIndividualMap().get(siblingId);
                    if (!individual.getPhenotypes().contains(phenotype)) {
                        // Sibling unaffected, continue
                        continue;
                    }

                    Genotype fatherGt = genotypes.get(individual.getFather().getId());
                    Genotype motherGt = genotypes.get(individual.getMother().getId());
                    Genotype childGt = genotypes.get(individual.getId());

                    // Exclude mendelian errors
                    if (MendelianError.compute(fatherGt, motherGt, childGt, chrom) > 0) {
                        continue;
                    }

                    // Update counter according to the genotypes
                    tdtCounters.update(childGt, fatherGt, motherGt);
                }
            }
        }

        double tdtChisq = -1;

        // Basic TDT test
        int t1 = tdtCounters.getT1();
        int t2 = tdtCounters.getT2();
        if (t1 + t2 > 0) {
            tdtChisq = ((double) ((t1 - t2) * (t1 - t2))) / (t1 + t2);
        }

        return new TdtTestResult(tdtChisq, (t2 == 0.0) ? -1 : ((double) t1 / t2), -1, 1, t1, t2);
    }

    public TdtTestResult computeTdtTest(ObjectMap families, Map<String, String> genotypes, Set<String> affectedSamples, String chrom) {
        Set<String> fatherMotherDone = new HashSet<>();

        // Transmission counts
        TdtCounters tdtCounters = new TdtCounters();

        // For each family...
        for (Map.Entry<String, Object> familyEntry : families.entrySet()) {
            ObjectMap familyValue = (ObjectMap) familyEntry.getValue();
            // For each sample...
            for (Map.Entry<String, Object> sampleEntry : familyValue.entrySet()) {
                String sample = sampleEntry.getKey();
                ObjectMap sampleValue = (ObjectMap) sampleEntry.getValue();
                String father = (String) sampleValue.getOrDefault("father", null);
                String mother = (String) sampleValue.getOrDefault("mother", null);

                // We need affected samples
                if (!affectedSamples.contains(sample)) {
                    continue;
                }

                // We need father and mother
                if (StringUtils.isEmpty(father) || StringUtils.isEmpty(mother)) {
                    continue;
                }

                // Check if these nuclear family has already been processed
                if (fatherMotherDone.contains(father + "_" + mother)) {
                    continue;
                }

                // Get genotype for father and mother and
                Genotype fatherGt = new Genotype(genotypes.get(father));
                Genotype motherGt = new Genotype(genotypes.get(mother));

                GenotypeCode fatherGtCode = getAlternateAlleleCount(fatherGt);
                GenotypeCode motherGtCode = getAlternateAlleleCount(motherGt);

                // At least one parent has to be heterozygous
                if (fatherGtCode != GenotypeCode.HET && motherGtCode != GenotypeCode.HET) {
                    continue;
                }

                List<String> siblingIds = new ArrayList<>();
                siblingIds.add(sample);
                siblingIds.addAll((List<String>) sampleValue.getOrDefault("siblings", null));

                // Init tmp counters
                tdtCounters.initCounters();

                // Consider all offspring in nuclear family
                for (String sibling : siblingIds) {
                    if (!affectedSamples.contains(sibling)) {
                        // Sibling unaffected, continue
                        continue;
                    }

                    Genotype childGt = new Genotype(genotypes.get(sample));
                    GenotypeCode childGtCode = getAlternateAlleleCount(childGt);

                    // Exclude mendelian errors
                    if (MendelianError.compute(fatherGt, motherGt, childGt, chrom) > 0) {
                        continue;
                    }

                    // Update TDT counters
                    tdtCounters.update(childGt, fatherGt, motherGt);
                }
            }
        }

        double tdtChisq = -1;

        // Basic TDT test
        int t1 = tdtCounters.getT1();
        int t2 = tdtCounters.getT2();
        if (t1 + t2 > 0) {
            tdtChisq = ((double) ((t1 - t2) * (t1 - t2))) / (t1 + t2);
        }

        return new TdtTestResult(tdtChisq, (t2 == 0.0) ? -1 : ((double) t1 / t2), -1, 1, t1, t2);
    }

    public class TdtCounters {
        int t1;
        int t2;

        int trA;  // transmitted allele from first het parent
        int unA;  // untransmitted allele from first het parent

        int trB;  // transmitted allele from second het parent
        int unB;  // untransmitted allele from second het parent

        public TdtCounters() {
            t1 = 0;
            t2 = 0;
            initCounters();
        }

        public void initCounters() {
            trA = 0;  // transmitted allele from first het parent
            unA = 0;  // untransmitted allele from first het parent

            trB = 0;  // transmitted allele from second het parent
            unB = 0;  // untransmitted allele from second het parent
        }

        public void update(Genotype childGt, Genotype fatherGt, Genotype motherGt) {
            GenotypeCode fatherCode = getAlternateAlleleCount(fatherGt);
            GenotypeCode motherCode = getAlternateAlleleCount(motherGt);
            GenotypeCode childCode = getAlternateAlleleCount(childGt);

            // We've now established: no missing genotypes and at least one heterozygous parent

            if (childCode == GenotypeCode.HOM_REF) {
                // Child, HOM_REF = 0/0
                if (fatherCode == GenotypeCode.HET && motherCode == GenotypeCode.HET) {
                    trA = 1;
                    unA = 2;
                    trB = 1;
                    unB = 2;
                } else {
                    trA = 1;
                    unA = 2;
                }
            } else if (childCode == GenotypeCode.HOM_VAR) {
                // Child, HOM_VAR = 1/1
                if (fatherCode == GenotypeCode.HET && motherCode == GenotypeCode.HET) {
                    trA = 2;
                    unA = 1;
                    trB = 2;
                    unB = 1;
                } else {
                    trA = 2;
                    unA = 1;
                }
            } else {
                // Child, HET = 0/1, 1/0
                if (fatherCode == GenotypeCode.HET) {
                    // Father, HET = 0/1, 1/0
                    if (motherCode == GenotypeCode.HET) {
                        // Mother, HET = 0/1, 1/0
                        trA = 1;
                        trB = 2;
                        unA = 2;
                        unB = 1;
                    } else if (motherCode == GenotypeCode.HOM_VAR) {
                        trA = 2;
                        unA = 1;
                    } else {
                        trA = 1;
                        unA = 2;
                    }
                } else if (fatherCode == GenotypeCode.HOM_VAR) {
                    trA = 2;
                    unA = 1;
                } else {
                    trA = 1;
                    unA = 2;
                }
            }

            // We have now populated trA (first transmission) and possibly trB also

            ////////////////////////////////////////
            // Permutation? 50:50 flip (precomputed)
//                    if (permute) {
//                        if (flipA[f]) {
//                            int t = trA;
//                            trA = unA;
//                            unA = t;
//
//                            t = trB;
//                            trB = unB;
//                            unB = t;
//                        }
//                    }

            // Increment transmission counts
            if (trA == 1) {
                t1++;
            } else if (trA == 2) {
                t2++;
            }

            if (trB == 1) {
                t1++;
            } else if (trB == 2) {
                t2++;
            }
        }

        public int getT1() {
            return t1;
        }

        public int getT2() {
            return t2;
        }
    }
}
