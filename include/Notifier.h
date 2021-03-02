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

#ifndef NOTIFIER_H
#define NOTIFIER_H

#include "FsMutationsBatcher.h"
#include "SchemabasedMetadataBatcher.h"
#include "ProjectsElasticSearch.h"
#include "HopsworksOpsLogTailer.h"
#include "MetadataLogTailer.h"
#include "ClusterConnectionBase.h"
#include "hive/TBLSTailer.h"
#include "hive/SDSTailer.h"
#include "hive/ISCHEMATailer.h"
#include "hive/SERDESTailer.h"
#include "hive/CDSTailer.h"
#include "hive/DBSTailer.h"
#include "hive/PARTTailer.h"
#include "hive/IDXSTailer.h"
#include "hive/SkewedLocTailer.h"
#include "hive/SkewedValuesTailer.h"
#include "http/server/HttpServer.h"
#include "FileProvenanceElastic.h"
#include "FileProvenanceElasticDataReader.h"
#include "AppProvenanceElastic.h"
#include "AppProvenanceElasticDataReader.h"

class Notifier : public ClusterConnectionBase {
public:
  Notifier(const char* connection_string, const char* database_name,
          const char* meta_database_name, const char* hive_meta_database_name,
          const TableUnitConf mutations_tu, const TableUnitConf schemabased_tu,const TableUnitConf provenance_tu,
          const int poll_maxTimeToWait, const HttpClientConfig elastic_client_config, const bool hopsworks,
          const std::string elastic_search_index, const std::string elastic_featurestore_index,
          const std::string elastic_app_provenance_index, const std::string ml_index,
          const int elastic_batch_size, const int elastic_issue_time,
          const int lru_cap, const int prov_file_lru_cap, const int prov_core_lru_cap, const bool recovery, const bool stats,
          Barrier barrier, const bool hiveCleaner, const std::string
          metricsServer);
  void start();
  virtual ~Notifier();

private:

  const TableUnitConf mMutationsTU;
  const TableUnitConf mSchemabasedTU;
  const TableUnitConf mFileProvenanceTU;
  const TableUnitConf mAppProvenanceTU;

  const int mPollMaxTimeToWait;
  const HttpClientConfig mElasticClientConfig;
  const bool mHopsworksEnabled;
  const std::string mElasticSearchIndex;
  const std::string mElasticFeaturestoreIndex;
  const std::string mElasticAppProvenanceIndex;
  const std::string mMLIndex;
  const int mElasticBatchsize;
  const int mElasticIssueTime;
  const int mLRUCap;
  const int mProvFileLRUCap;
  const int mProvCoreLRUCap;
  const bool mRecovery;
  const bool mStats;
  const Barrier mBarrier;
  const bool mHiveCleaner;
  const std::string mMetricsServer;

  ProjectsElasticSearch* mProjectsElasticSearch;

  FsMutationsTableTailer* mFsMutationsTableTailer;
  FsMutationsDataReaders* mFsMutationsDataReaders;
  FsMutationsBatcher* mFsMutationsBatcher;

  MetadataLogTailer* mMetadataLogTailer;

  SchemabasedMetadataReaders* mSchemabasedMetadataReaders;
  SchemabasedMetadataBatcher* mSchemabasedMetadataBatcher;

  HopsworksOpsLogTailer* mhopsworksOpsLogTailer;

  FileProvenanceTableTailer* mFileProvenanceTableTailer;
  FileProvenanceElasticDataReaders* mFileProvenanceElasticDataReaders;
  RCBatcher<FileProvenanceRow, SConn>* mFileProvenanceBatcher;
  FileProvenanceElastic* mFileProvenanceElastic;

  AppProvenanceTableTailer* mAppProvenanceTableTailer;
  AppProvenanceElasticDataReaders* mAppProvenanceElasticDataReaders;
  RCBatcher<AppProvenanceRow, SConn>* mAppProvenanceBatcher;
  AppProvenanceElastic* mAppProvenanceElastic;

  TBLSTailer* mTblsTailer;
  SDSTailer* mSDSTailer;
  ISCHEMATailer* mISCHEMATailer;
  SERDESTailer* mSERDESTailer;
  CDSTailer* mCDSTailer;
  DBSTailer* mDBSTailer;
  PARTTailer* mPARTTailer;
  IDXSTailer* mIDXSTailer;
  SkewedLocTailer* mSkewedLocTailer;
  SkewedValuesTailer* mSkewedValuesTailer;

  HttpServer* mHttpServer;
  MetricsProviders* mMetricsProviders;
  void setup();
};

#endif /* NOTIFIER_H */

