package net.michaelkoepf.spegauge.api.common.model.sqbench;

import net.michaelkoepf.spegauge.api.sut.BitSet;

public class EntityRecordLimitedAttrs extends EntityRecordWithQuerySet {
    public boolean isOfTypeA;
    public int intAttribute; // join key or group by key

    public EntityRecordLimitedAttrs() {
    }

    public EntityRecordLimitedAttrs(boolean isOfTypeA, int intAttribute, int filterAttribute, int numOfQueries) {
        this.isOfTypeA = isOfTypeA;
        this.intAttribute = intAttribute;
        this.filterAttribute = filterAttribute;
        this.querySet = new BitSet(numOfQueries);
    }
}
