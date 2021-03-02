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

#ifndef FILEPROVENANCEELASTICDATAREADER_H
#define FILEPROVENANCEELASTICDATAREADER_H

#include "NdbDataReaders.h"
#include "FileProvenanceTableTailer.h"
#include "boost/date_time.hpp"
#include "boost/date_time/posix_time/posix_time.hpp"
#include "rapidjson/document.h"
#include "rapidjson/error/en.h"
#include "tables/XAttrTable.h"
#include "tables/FileProvenanceXAttrBufferTable.h"
#include "tables/INodeTable.h"
#include "FileProvenanceConstants.h"
#include "FileProvenanceElastic.h"

struct ProcessRowResult {
  std::list<std::string> mElasticOps;
  FileProvenancePK mLogPK;
  boost::optional<FPXAttrBufferPK> mCompanionPK;
  FileProvenanceConstantsRaw::Operation mProvOp;
};

class FileProvenanceElasticDataReader : public NdbDataReader<FileProvenanceRow, SConn> {
public:
  FileProvenanceElasticDataReader(SConn hopsConn, const bool hopsworks, int prov_file_lru_cap, int prov_core_lru_cap, int inodes_lru_cap, const std::string ml_index);
  virtual ~FileProvenanceElasticDataReader();
protected:

private:
  FileProvenanceLogTable mFileLogTable;
  INodeTable inodesTable;
  std::string mMLIndex;

  void processAddedandDeleted(Pq* data_batch, eBulk& bulk);
  ProcessRowResult rowResult(std::list<std::string> elasticOps, FileProvenancePK logPK,
          boost::optional<FPXAttrBufferPK> companionPK, FileProvenanceConstantsRaw::Operation provOp);
  ProcessRowResult process_row(FileProvenanceRow row);
  bool projectExists(Int64 projectIId, Int64 timestamp);
  FPXAttrBufferRow readBufferedXAttr(FPXAttrBufferPK xattrBufferKey);
  boost::optional<FPXAttrBufferRow> getProvCore(Int64 inodeId, int inodeLogicalTime);
  boost::optional<FPXAttrBufferRow> readProvCore(Int64 inodeId, int fromLogicalTime, int toLogicalTime);
  ULSet getViewInodes(Pq* data_batch);
  std::string getElasticBulkOps(std::list <std::string> bulkOps);
};

class FileProvenanceElasticDataReaders :  public NdbDataReaders<FileProvenanceRow, SConn>{
  public:
    FileProvenanceElasticDataReaders(SConn* hopsConns, int num_readers,const bool hopsworks,
          TimedRestBatcher* restEndpoint, int prov_file_lru_cap, int prov_core_lru_cap, int inodes_lru_ca, const std::string ml_index) :
    NdbDataReaders(restEndpoint){
      for(int i=0; i<num_readers; i++){
        FileProvenanceElasticDataReader* dr
          = new FileProvenanceElasticDataReader(hopsConns[i], hopsworks, prov_file_lru_cap, prov_core_lru_cap, inodes_lru_ca, ml_index);
        dr->start(i, this);
        mDataReaders.push_back(dr);
      }
    }
};

#endif /* FILEPROVENANCEELASTICDATAREADER_H */

