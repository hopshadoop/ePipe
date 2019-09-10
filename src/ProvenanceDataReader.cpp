/*
 * This file is part of ePipe
 * Copyright (C) 2019, Logical Clocks AB. All rights reserved
 *
 * ePipe is free software: you can redistribute it and/or modify it under the terms of
 * the GNU Affero General Public License as published by the Free Software Foundation,
 * either version 3 of the License, or (at your option) any later version.
 *
 * ePipe is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE.  See the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this program.
 * If not, see <https://www.gnu.org/licenses/>.
 */

#include "ProvenanceDataReader.h"

ProvenanceDataReader::ProvenanceDataReader(SConn connection, const bool hopsworks)
: NdbDataReader(connection, hopsworks) {
}

void ProvenanceDataReader::processAddedandDeleted(Pq* data_batch, eBulk& bulk) {
  for (Pq::iterator it = data_batch->begin(); it != data_batch->end(); ++it) {
    ProvenanceRow row = *it;
    bulk.push(mProvenanceLogTable.getLogRemovalHandler(row), row
    .mEventCreationTime, row.to_create_json());
  }
}

ProvenanceDataReader::~ProvenanceDataReader() {
  
}

