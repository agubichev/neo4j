/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.consistency.checking.full;

import org.neo4j.consistency.checking.CheckerEngine;
import org.neo4j.consistency.checking.RecordCheck;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.store.DiffRecordAccess;
import org.neo4j.consistency.store.RecordAccess;
import org.neo4j.kernel.api.labelscan.LabelScanReader;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;

public class NodeLabelCheck implements RecordCheck<NodeRecord, ConsistencyReport.LabelsMatchReport>
{
    private final LabelScanReader labelScanReader;

    public NodeLabelCheck( LabelScanReader labelScanReader )
    {
        this.labelScanReader = labelScanReader;
    }

    @Override
    public void check( NodeRecord record, CheckerEngine<NodeRecord, ConsistencyReport.LabelsMatchReport> engine,
                       RecordAccess records )
    {
        engine.comparativeCheck( records.node( record.getId() ), new LabelsMatchCheck( labelScanReader ) );
    }

    @Override
    public void checkChange( NodeRecord ignored, NodeRecord record, CheckerEngine<NodeRecord,
            ConsistencyReport.LabelsMatchReport> engine, DiffRecordAccess records )
    {
        check( record, engine, records );
    }
}
