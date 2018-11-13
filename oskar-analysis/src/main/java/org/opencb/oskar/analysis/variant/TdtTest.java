package org.opencb.oskar.analysis.variant;

import org.opencb.biodata.models.clinical.pedigree.Member;
import org.opencb.biodata.models.clinical.pedigree.Pedigree;
import org.opencb.biodata.models.clinical.pedigree.PedigreeManager;
import org.opencb.biodata.models.commons.Phenotype;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.oskar.analysis.variant.MendelianError.GenotypeCode;

import java.util.*;

import static org.opencb.oskar.analysis.variant.MendelianError.getAlternateAlleleCount;

public class TdtTest {

    public TdtTestResult computeTdtTest(Map<String, Genotype> genotypes, List<Pedigree> pedigrees, Phenotype phenotype,
                                        String chrom) {
        Set<String> fatherMotherDone = new HashSet<>();

        // Transmission counts
        int t1 = 0;
        int t2 = 0;

        // For each family
        for (Pedigree pedigree: pedigrees) {
            PedigreeManager pedigreeManager = new PedigreeManager(pedigree);
            Set<Member> affectedIndividuals = pedigreeManager.getAffectedIndividuals(phenotype);

            for (Member affectedIndividual: affectedIndividuals) {
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

                int trA = 0;  // transmitted allele from first het parent
                int unA = 0;  // untransmitted allele from first het parent

                int trB = 0;  // transmitted allele from second het parent
                int unB = 0;  // untransmitted allele from second het parent

                // Consider all offspring in nuclear family
                for (String siblingId: siblingIds) {
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
            }
        }

        double tdtChisq = -1;

        // Basic TDT test
        if (t1 + t2 > 0) {
            tdtChisq = ((double) ((t1 - t2) * (t1 - t2))) / (t1 + t2);
        }

        return new TdtTestResult(tdtChisq, (t2 == 0.0) ? -1 : ((double) t1 / t2), -1, 1, t1, t2);
    }
}
