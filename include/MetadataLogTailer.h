/*
 * This file is part of ePipe
 * Copyright (C) 2019, Logical Clocks AB. All rights reserved
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */

#ifndef METADATALOGTAILER_H
#define METADATALOGTAILER_H

#include "RCTableTailer.h"
#include "HopsworksOpsLogTailer.h"
#include "tables/MetadataLogTable.h"

class MetadataLogTailer : public RCTableTailer<MetadataLogEntry> {
public:
  MetadataLogTailer(Ndb* ndb, Ndb* ndbRecovery, const int poll_maxTimeToWait,
      const Barrier barrier);
  MetadataLogEntry consume();

  virtual ~MetadataLogTailer();
private:

  virtual void handleEvent(NdbDictionary::Event::TableEvent eventType, MetadataLogEntry pre, MetadataLogEntry row);
  CMetaQ* mSchemaBasedQueue;
};

#endif /* METADATALOGTAILER_H */

