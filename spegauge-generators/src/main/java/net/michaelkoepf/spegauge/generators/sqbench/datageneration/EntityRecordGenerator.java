package net.michaelkoepf.spegauge.generators.sqbench.datageneration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.*;
import net.michaelkoepf.spegauge.api.common.model.sqbench.EntityRecordFull;
import net.michaelkoepf.spegauge.generators.sqbench.common.SQBenchUtils;
import org.apache.commons.numbers.gamma.RegularizedGamma;
import org.apache.commons.rng.UniformRandomProvider;
import org.apache.commons.statistics.distribution.ContinuousDistribution;
import org.apache.commons.statistics.distribution.DiscreteDistribution;
import org.apache.commons.statistics.distribution.UniformDiscreteDistribution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Base64;
import java.util.List;

/**
 * Generates records of a specific entity.
 */
public final class EntityRecordGenerator {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final Logger LOGGER =
            LoggerFactory.getLogger(EntityRecordGenerator.class);

    // use a offset to make records same length when they are transferred to the SUT as strings
    private static final long ID_OFFSET = 100_000_000L;
    private static final long LONG_ATTR_OFFSET = 0L;

    private final EntityRecordFull.Type entityType;

    // selectivity attributes (should always be sampled from uniform distribution so selectivity can be easily controlled)
    // E.g., with lower 1 and upper 10, a selectivity of 0.2 can be achieved by comparing the selectivity field
    // to <= 2.
    private static final int SELECTIVITY_LOWER_INCLUSIVE = 0;
    private static final int SELECTIVITY_UPPER_INCLUSIVE = 9;
    private final UniformRandomProvider rng;

    private SQBenchUtils.Entity entity;
    private DiscreteDistribution.Sampler selectivyAttribute1Sampler;
    private DiscreteDistribution.Sampler selectivyAttribute2Sampler;
    private UnionDistributionSampler distributionSamplerWrapperPK;

    // PK
    @Getter
    private long[] PKs;

    // FK
    @Getter
    private EntityRecordFull.Type referencedEntityType;

    @Setter
    private long[] FKs = null; // set when all entities are initialized

    private UnionDistributionSampler distributionSamplerWrapperFK;


    // long attribute 1
    private long[] longAttribute1;

    private UnionDistributionSampler distributionSamplerWrapperLongAttribute1;

    // long attribute 2
    private long[] longAttribute2;

    private UnionDistributionSampler distributionSamplerWrapperLongAttribute2;

    // filter attribute
    private UnionDistributionSampler filterAttributeSampler;

    // string attributes
    private int plainStringLength;

    // payload type
    private boolean jsonStringAttribute;
    private boolean xmlStringAttribute;

    // ATTENTION: padding may differ when copying and entity generator due to initial buffers
    // this attribute is only used to simulate different records sizes
    private byte[] padding;

    private final int numOfQueries;

    public EntityRecordGenerator(UniformRandomProvider rng, SQBenchUtils.Entity entity, int numOfQueries) {
        // these attributes remain unchanged during the lifetime of the generator
        this.rng = rng;
        this.entity = entity;

        this.referencedEntityType = (this.entity.FK != null ? this.entity.FK.references : null);
        this.entityType = entity.entityType;
        this.selectivyAttribute1Sampler = UniformDiscreteDistribution.of(SELECTIVITY_LOWER_INCLUSIVE, SELECTIVITY_UPPER_INCLUSIVE).createSampler(this.rng);
        this.selectivyAttribute2Sampler = UniformDiscreteDistribution.of(SELECTIVITY_LOWER_INCLUSIVE, SELECTIVITY_UPPER_INCLUSIVE).createSampler(this.rng);

        this.numOfQueries = numOfQueries;
    }

    public static EntityRecordGenerator copy(EntityRecordGenerator toBeCopied, UniformRandomProvider rng) {
        return new EntityRecordGenerator(rng, toBeCopied.entity, toBeCopied.numOfQueries);
    }

    public void setAttributes(SQBenchUtils.Entity entity) {
        // check if entityType and referencedEntityType are consistent
        if (this.entityType != entity.entityType) {
            throw new IllegalArgumentException("Entity type mismatch. Entity type cannot change over lifetime of a Generator");
        }

        if (this.referencedEntityType != (entity.FK != null ? entity.FK.references : null)) {
            throw new IllegalArgumentException("Referenced entity type mismatch. Referenced entity type cannot change over lifetime of a Generator");
        }

        // update attributes
        this.entity = entity;

        // PK
        PKs = EntityRecordGeneratorUtils.getConsecutiveLongValues(ID_OFFSET, this.entity.PK.numDistinctValues);
        this.distributionSamplerWrapperPK = new UnionDistributionSampler(this.entity.PK.distribution.type, this.entity.PK.distribution.parameters, this.rng);

        // FK (values can only be set after all entities have been initialized)
        if (this.referencedEntityType != null) {
            this.distributionSamplerWrapperFK = new UnionDistributionSampler(this.entity.FK.distribution.type, this.entity.FK.distribution.parameters, this.rng);
        }

        // long attributes
        this.longAttribute1 = EntityRecordGeneratorUtils.getConsecutiveLongValues(LONG_ATTR_OFFSET, this.entity.longAttribute1.numDistinctValues);
        this.distributionSamplerWrapperLongAttribute1 = new UnionDistributionSampler(this.entity.longAttribute1.distribution.type, this.entity.longAttribute1.distribution.parameters, this.rng);

        this.longAttribute2 = EntityRecordGeneratorUtils.getConsecutiveLongValues(LONG_ATTR_OFFSET, this.entity.longAttribute2.numDistinctValues);
        this.distributionSamplerWrapperLongAttribute2 = new UnionDistributionSampler(this.entity.longAttribute2.distribution.type, this.entity.longAttribute2.distribution.parameters, this.rng);

        // filter attribute
        if (this.entity.filterAttribute != null) {
            this.filterAttributeSampler = new UnionDistributionSampler(this.entity.filterAttribute.distribution.type, this.entity.filterAttribute.distribution.parameters, this.rng);
        }

        // string attributes
        this.plainStringLength = this.entity.plainStringAttributeLength.length;
        this.jsonStringAttribute = this.entity.jsonStringAttribute.enabled;
        this.xmlStringAttribute = this.entity.xmlStringAttribute.enabled;

        if (jsonStringAttribute && xmlStringAttribute) {
            throw new IllegalArgumentException("Only one of JSON and XML string attributes can be enabled");
        }

        // padding
        this.padding = new byte[this.entity.numBytesPadding];
    }

    public EntityRecordFull nextEntity(long uniqueEventId, long eventTimeStamp) throws JsonProcessingException {
        // PK
        long PK = PKs[distributionSamplerWrapperPK.sample(PKs.length)];

        // FK
        long FK = -1;
        if (FKs != null) {
            FK = FKs[distributionSamplerWrapperFK.sample(FKs.length)];
        }

        // selectivity attributes
        long selectivityAttribute1Sampled = selectivyAttribute1Sampler.sample();
        long selectivityAttribute2Sampled = selectivyAttribute2Sampler.sample();

        // long attributes
        long longAttribute1Sampled = longAttribute1[distributionSamplerWrapperLongAttribute1.sample(longAttribute1.length)];
        long longAttribute2Sampled = longAttribute2[distributionSamplerWrapperLongAttribute2.sample(longAttribute2.length)];

        // filter attribute
        Integer filterAttributeSampled = null;
        if (this.filterAttributeSampler != null) {
            filterAttributeSampled = filterAttributeSampler.sample(0);
        }

        // string attributes
        String plainString = EntityRecordGeneratorUtils.generateRandomString(rng, plainStringLength);

        // padding
        rng.nextBytes(padding);

        if (jsonStringAttribute) {
            ObjectNode rootNode = objectMapper.createObjectNode();
            rootNode.put("type", entityType.toString());
            rootNode.put("PK", PK);
            rootNode.put("FK", FK);
            rootNode.put("selectivityAttribute1", selectivityAttribute1Sampled);
            rootNode.put("selectivityAttribute2", selectivityAttribute2Sampled);
            rootNode.put("longAttribute1", longAttribute1Sampled);
            rootNode.put("longAttribute2", longAttribute2Sampled);

            rootNode.put("filterAttribute", filterAttributeSampled);
            rootNode.put("plainString", plainString);
            rootNode.put("padding", Base64.getEncoder().encodeToString(padding));

            return new EntityRecordFull(EntityRecordFull.Type.JSON, uniqueEventId, eventTimeStamp, Instant.now().toEpochMilli(), rootNode.toString(), numOfQueries);
        }

        if (xmlStringAttribute) {
            throw new UnsupportedOperationException("XML string generation not yet implemented");
        }

        return new EntityRecordFull(entityType, uniqueEventId, eventTimeStamp, Instant.now().toEpochMilli(), PK, FK, selectivityAttribute1Sampled, selectivityAttribute2Sampled, longAttribute1Sampled, longAttribute2Sampled, filterAttributeSampled, plainString, null, null, padding, numOfQueries);
    }

    private static class UnionDistributionSampler {
        private final ContinuousDistribution.Sampler continuousDistributionSampler;
        private final DiscreteDistribution.Sampler discreteDistributionSampler;
        private final SQBenchUtils.Distribution.Type type;

        private final int valueOffset;

        // used if we want the most frequent element of the zipf distribution to be different from 0
        private int numOfElements = 0;
        private int mostFreqElement = 0;

        public UnionDistributionSampler(SQBenchUtils.Distribution.Type type, List<Double> parameters, UniformRandomProvider rng) {
            this.type = type;
            this.continuousDistributionSampler = SQBenchUtils.Distribution.getContinuousDistributionSampler(type, parameters, rng);
            this.discreteDistributionSampler = SQBenchUtils.Distribution.getDiscreteDistributionSampler(type, parameters, rng);

            if (this.continuousDistributionSampler == null && this.discreteDistributionSampler == null) {
                throw new UnsupportedOperationException("Distribution type not supported: " + type);
            }

//            if (this.continuousDistributionSampler != null) {
//                throw new UnsupportedOperationException("Continuous distributions not yet supported");
//            }

            if (this.type == SQBenchUtils.Distribution.Type.ZIPF) {
                if (parameters.size() == 3){
                    this.mostFreqElement = parameters.get(2).intValue();
                    this.numOfElements = parameters.get(0).intValue();

                }
                this.valueOffset = 1;
            } else {
                this.valueOffset = 0;
            }
        }

        public int sample(int n) {
            if (continuousDistributionSampler != null) {
                return (int) Math.round(continuousDistributionSampler.sample() * (n-1));
            } else {
                int frequency = discreteDistributionSampler.sample() - valueOffset;
                int element;
                if (mostFreqElement != 0) {
                    element = frequency + mostFreqElement;
                    if (element >= numOfElements) {
                        element = element - numOfElements;
                    }
                }
                else {
                    element = frequency;
                }
                return element;
            }
        }
    }

}