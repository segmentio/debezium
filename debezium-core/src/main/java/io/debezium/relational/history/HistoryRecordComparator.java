/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.history;

import java.util.function.BiFunction;

import io.debezium.document.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Compares HistoryRecord instances to determine which came first.
 * 
 * @author Randall Hauch
 * @since 0.2
 */
public class HistoryRecordComparator {
    protected final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * A comparator instance that requires the {@link HistoryRecord#source() records' sources} to be the same and considers only
     * those fields that are in both records' {@link HistoryRecord#position() positions}.
     */
    public static final HistoryRecordComparator INSTANCE = new HistoryRecordComparator();

    /**
     * Create a {@link HistoryRecordComparator} that requires identical sources but will use the supplied function to compare
     * positions.
     * 
     * @param positionComparator the non-null function that returns {@code true} if the first position is at or before
     *            the second position or {@code false} otherwise
     * @return the comparator instance; never null
     */
    public static HistoryRecordComparator usingPositions(BiFunction<Document, Document, Boolean> positionComparator) {
        return new HistoryRecordComparator() {
            @Override
            protected boolean isPositionAtOrBefore(Document position1, Document position2) {
                return positionComparator.apply(position1, position2);
            }
        };
    }

    /**
     * Determine if the first {@link HistoryRecord} is at the same or earlier point in time than the second {@link HistoryRecord}.
     * 
     * @param record1 the first record; never null
     * @param record2 the second record; never null
     * @return {@code true} if the first record is at the same or earlier point in time than the second record, or {@code false}
     *         otherwise
     */
    public boolean isAtOrBefore(HistoryRecord record1, HistoryRecord record2) {
        boolean sourceSame = isSameSource(record1.source(), record2.source());
        boolean positionOk = isPositionAtOrBefore(record1.position(), record2.position());
        if (!sourceSame) {
            logger.debug("isAtOrBefore: source is not same ({} != {})", record1.source(), record2.source());
        }
        if (!positionOk) {
            logger.debug("isAtOrBefore: position is not ok ({} vs {})", record1.position(), record2.position());
        }
        return sourceSame && positionOk;
    }

    protected boolean isPositionAtOrBefore(Document position1, Document position2) {
        return position1.compareToUsingSimilarFields(position2) <= 0;
    }

    protected boolean isSameSource(Document source1, Document source2) {
        return source1.equals(source2);
    }
}