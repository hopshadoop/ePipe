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

#include "Notifier.h"

Notifier::Notifier(const char* connection_string, const char* database_name,
    const char* meta_database_name, const char* hive_meta_database_name,
        const TableUnitConf mutations_tu, const TableUnitConf schemabased_tu,
        const TableUnitConf elastic_provenance_tu, const int poll_maxTimeToWait,
        const HttpClientConfig elastic_client_config, const bool hopsworks,
        const std::string elastic_search_index, const std::string elastic_featurestore_index,
        const std::string elastic_app_provenance_index, const std::string ml_index,
                   const int elastic_batch_size, const int elastic_issue_time,
        const int lru_cap, const int prov_file_lru_cap, const int prov_core_lru_cap, const bool recovery,
        const bool stats, Barrier barrier, const bool hiveCleaner, const
        std::string metricsServer)
: ClusterConnectionBase(connection_string, database_name, meta_database_name, hive_meta_database_name), 
    mMutationsTU(mutations_tu), mSchemabasedTU(schemabased_tu),
    mFileProvenanceTU(elastic_provenance_tu), mAppProvenanceTU(elastic_provenance_tu), 
    mPollMaxTimeToWait(poll_maxTimeToWait),  mElasticClientConfig(elastic_client_config), mHopsworksEnabled(hopsworks),
    mElasticSearchIndex(elastic_search_index), mElasticFeaturestoreIndex(elastic_featurestore_index),
    mElasticAppProvenanceIndex(elastic_app_provenance_index), mMLIndex(ml_index),
    mElasticBatchsize(elastic_batch_size), mElasticIssueTime(elastic_issue_time),
    mLRUCap(lru_cap), mProvFileLRUCap(prov_file_lru_cap), mProvCoreLRUCap(prov_core_lru_cap),
    mRecovery(recovery), mStats(stats), mBarrier(barrier), mHiveCleaner(hiveCleaner), mMetricsServer(metricsServer) {
  setup();
}

void Notifier::start() {
  LOG_INFO("ePipe starting...");
  ptime t1 = getCurrentTime();

  if (mMutationsTU.isEnabled()) {
    mFsMutationsDataReaders->start();
    mFsMutationsBatcher->start();
    mFsMutationsTableTailer->start();
  }

  if (mSchemabasedTU.isEnabled()) {
    mSchemabasedMetadataReaders->start();
    mSchemabasedMetadataBatcher->start();
    mMetadataLogTailer->start();
  }

  if (mMutationsTU.isEnabled() || mSchemabasedTU.isEnabled()
           || mHopsworksEnabled) {
    mProjectsElasticSearch->start();
  }

  if (mHopsworksEnabled) {
    mhopsworksOpsLogTailer->start();
  }

  if (mFileProvenanceTU.isEnabled()) {
    mFileProvenanceElastic->start();
    mFileProvenanceElasticDataReaders->start();
    mFileProvenanceBatcher->start();
    mFileProvenanceTableTailer->start();
  }
  if(mAppProvenanceTU.isEnabled()) {
    mAppProvenanceElastic->start();
    mAppProvenanceElasticDataReaders->start();
    mAppProvenanceBatcher->start();
    mAppProvenanceTableTailer->start();
  }

  if(mHiveCleaner) {
    mTblsTailer->start();
    mSDSTailer->start();
    mISCHEMATailer->start();
    mDBSTailer->start();
    mSERDESTailer->start();
    mCDSTailer->start();
    mPARTTailer->start();
    mIDXSTailer->start();
    mSkewedLocTailer->start();
    mSkewedValuesTailer->start();
  }

  ptime t2 = getCurrentTime();
  LOG_INFO("ePipe started in " << getTimeDiffInMilliseconds(t1, t2) << " msec");

  if(mStats) {
    mHttpServer->run();
  }

  if (mMutationsTU.isEnabled()) {
    mFsMutationsBatcher->waitToFinish();
    mFsMutationsTableTailer->waitToFinish();
  }

  if (mSchemabasedTU.isEnabled()) {
    mSchemabasedMetadataBatcher->waitToFinish();
    mMetadataLogTailer->waitToFinish();
  }

  if (mMutationsTU.isEnabled() || mSchemabasedTU.isEnabled()
           || mHopsworksEnabled) {
    mProjectsElasticSearch->waitToFinish();
  }

  if (mHopsworksEnabled) {
    mhopsworksOpsLogTailer->waitToFinish();
  }

  if (mFileProvenanceTU.isEnabled()) {
    mFileProvenanceBatcher->waitToFinish();
    mFileProvenanceTableTailer->waitToFinish();
    mFileProvenanceElastic->waitToFinish();
  }
  if (mAppProvenanceTU.isEnabled()) {
    mAppProvenanceBatcher->waitToFinish();
    mAppProvenanceTableTailer->waitToFinish();
    mAppProvenanceElastic->waitToFinish();
  }

  if(mHiveCleaner) {
    mTblsTailer->waitToFinish();
    mSDSTailer->waitToFinish();
    mISCHEMATailer->waitToFinish();
    mDBSTailer->waitToFinish();
    mSERDESTailer->waitToFinish();
    mCDSTailer->waitToFinish();
    mPARTTailer->waitToFinish();
    mIDXSTailer->waitToFinish();
    mSkewedLocTailer->waitToFinish();
    mSkewedValuesTailer->waitToFinish();
  }
}

void Notifier::setup() {
  if (mMutationsTU.isEnabled() || mSchemabasedTU.isEnabled() ||
  mHopsworksEnabled) {
    MConn ndb_connections_elastic;
    ndb_connections_elastic.metadataConnection = create_ndb_connection(mMetaDatabaseName);
    ndb_connections_elastic.inodeConnection = create_ndb_connection(mDatabaseName);

    mProjectsElasticSearch = new ProjectsElasticSearch(mElasticClientConfig,
            mElasticIssueTime, mElasticBatchsize, mStats, ndb_connections_elastic);
  }


  if (mMutationsTU.isEnabled()) {
    Ndb* mutations_tailer_connection = create_ndb_connection(mDatabaseName);
    Ndb* mutations_tailer_recovery_connection = mRecovery ?
        create_ndb_connection(mDatabaseName) : nullptr;

    mFsMutationsTableTailer = new FsMutationsTableTailer(mutations_tailer_connection,
        mutations_tailer_recovery_connection, mPollMaxTimeToWait, mBarrier);

    MConn* mutations_connections = new MConn[mMutationsTU.mNumReaders];
    for (int i = 0; i < mMutationsTU.mNumReaders; i++) {
      mutations_connections[i].inodeConnection = create_ndb_connection(mDatabaseName);
      mutations_connections[i].metadataConnection = create_ndb_connection(mMetaDatabaseName);
    }

    mFsMutationsDataReaders = new FsMutationsDataReaders(mutations_connections, mMutationsTU.mNumReaders,
            mHopsworksEnabled, mProjectsElasticSearch, mLRUCap, mElasticSearchIndex, mElasticFeaturestoreIndex);
    mFsMutationsBatcher = new FsMutationsBatcher(mFsMutationsTableTailer, mFsMutationsDataReaders,
            mMutationsTU.mWaitTime, mMutationsTU.mBatchSize);
  }

  if (mSchemabasedTU.isEnabled()) {

    Ndb* metadata_tailer_connection = create_ndb_connection(mMetaDatabaseName);
    Ndb* metadata_tailer_recovery_connection = mRecovery ? create_ndb_connection
        (mMetaDatabaseName) : nullptr;
    mMetadataLogTailer = new MetadataLogTailer(metadata_tailer_connection,
        metadata_tailer_recovery_connection,
        mPollMaxTimeToWait, mBarrier);

    MConn* metadata_connections = new MConn[mSchemabasedTU.mNumReaders];
    for (int i = 0; i < mSchemabasedTU.mNumReaders; i++) {
      metadata_connections[i].inodeConnection = create_ndb_connection(mDatabaseName);
      metadata_connections[i].metadataConnection = create_ndb_connection(mMetaDatabaseName);
    }

    mSchemabasedMetadataReaders = new SchemabasedMetadataReaders(metadata_connections, mSchemabasedTU.mNumReaders,
            mHopsworksEnabled, mProjectsElasticSearch, mLRUCap);
    mSchemabasedMetadataBatcher = new SchemabasedMetadataBatcher(mMetadataLogTailer, mSchemabasedMetadataReaders,
            mSchemabasedTU.mWaitTime, mSchemabasedTU.mBatchSize);
  }

  if (mHopsworksEnabled) {
    Ndb* ops_log_tailer_connection = create_ndb_connection(mMetaDatabaseName);
    Ndb* ops_log_tailer_recovery_connection = mRecovery ? create_ndb_connection
        (mMetaDatabaseName) : nullptr;
    mhopsworksOpsLogTailer = new HopsworksOpsLogTailer(ops_log_tailer_connection,
        ops_log_tailer_recovery_connection, mPollMaxTimeToWait, mBarrier,
            mProjectsElasticSearch, mLRUCap, mElasticSearchIndex);
  }

  if (mFileProvenanceTU.isEnabled()) {
    //file
    Ndb* ndb_elastic_file_provenance_conn = create_ndb_connection(mDatabaseName);
    mFileProvenanceElastic = new FileProvenanceElastic(mElasticClientConfig,
      mElasticIssueTime, mElasticBatchsize, mStats, ndb_elastic_file_provenance_conn, mProvFileLRUCap, mProvCoreLRUCap);

    Ndb* elastic_file_provenance_tailer_connection = create_ndb_connection(mDatabaseName);
    Ndb* elastic_file_provenance_tailer_recovery_connection = mRecovery ? create_ndb_connection(mDatabaseName) : nullptr;
    mFileProvenanceTableTailer = new FileProvenanceTableTailer(
        elastic_file_provenance_tailer_connection, elastic_file_provenance_tailer_recovery_connection,
        mPollMaxTimeToWait, mBarrier, mProvFileLRUCap, mProvCoreLRUCap);

    SConn* file_prov_hops_connections = new SConn[mFileProvenanceTU.mNumReaders];
    for (int i = 0; i < mFileProvenanceTU.mNumReaders; i++) {
      file_prov_hops_connections[i] = create_ndb_connection(mDatabaseName);
    }
    mFileProvenanceElasticDataReaders = new FileProvenanceElasticDataReaders(file_prov_hops_connections,
      mFileProvenanceTU.mNumReaders, mHopsworksEnabled, mFileProvenanceElastic, mProvFileLRUCap, mProvCoreLRUCap, mLRUCap, mMLIndex);
    mFileProvenanceBatcher = new RCBatcher<FileProvenanceRow, SConn>(
      mFileProvenanceTableTailer, mFileProvenanceElasticDataReaders,
      mFileProvenanceTU.mWaitTime, mFileProvenanceTU.mBatchSize);
  }
  if (mAppProvenanceTU.isEnabled()) {
    //app
    Ndb* ndb_elastic_app_provenance_conn = create_ndb_connection(mDatabaseName);
    mAppProvenanceElastic = new AppProvenanceElastic(mElasticClientConfig, mElasticAppProvenanceIndex,
      mElasticIssueTime, mElasticBatchsize, mStats, ndb_elastic_app_provenance_conn);

    Ndb* elastic_app_provenance_tailer_connection = create_ndb_connection(mDatabaseName);
    Ndb* elastic_app_provenance_tailer_recovery_connection = mRecovery ? create_ndb_connection(mDatabaseName) : nullptr;
    mAppProvenanceTableTailer = new AppProvenanceTableTailer(
        elastic_app_provenance_tailer_connection, elastic_app_provenance_tailer_recovery_connection,
        mPollMaxTimeToWait, mBarrier);

    SConn* elastic_app_provenance_connections = new SConn[mAppProvenanceTU.mNumReaders];
    for (int i = 0; i < mAppProvenanceTU.mNumReaders; i++) {
      elastic_app_provenance_connections[i] = create_ndb_connection(mDatabaseName);
    }
    mAppProvenanceElasticDataReaders = new AppProvenanceElasticDataReaders(elastic_app_provenance_connections, 
      mAppProvenanceTU.mNumReaders, mHopsworksEnabled, mAppProvenanceElastic);
    mAppProvenanceBatcher = new RCBatcher<AppProvenanceRow, SConn>(
      mAppProvenanceTableTailer, mAppProvenanceElasticDataReaders,
      mAppProvenanceTU.mWaitTime, mAppProvenanceTU.mBatchSize);
  }


  if(mHiveCleaner) {
    Ndb *tbls_tailer_connection = create_ndb_connection(mHiveMetaDatabaseName);
    mTblsTailer = new TBLSTailer(tbls_tailer_connection, mPollMaxTimeToWait,
        mBarrier);

    Ndb *sds_tailer_connection = create_ndb_connection(mHiveMetaDatabaseName);
    mSDSTailer = new SDSTailer(sds_tailer_connection, mPollMaxTimeToWait,
        mBarrier);

    Ndb *ischema_tailer_connection = create_ndb_connection(mHiveMetaDatabaseName);
    mISCHEMATailer = new ISCHEMATailer(ischema_tailer_connection, mPollMaxTimeToWait,
        mBarrier);

    Ndb *dbs_tailer_connection = create_ndb_connection(mHiveMetaDatabaseName);
    mDBSTailer = new DBSTailer(dbs_tailer_connection, mPollMaxTimeToWait,
        mBarrier);

    Ndb *serdes_tailer_connection = create_ndb_connection(mHiveMetaDatabaseName);
    mSERDESTailer = new SERDESTailer(serdes_tailer_connection, mPollMaxTimeToWait,
        mBarrier);

    Ndb *cds_tailer_connection = create_ndb_connection(mHiveMetaDatabaseName);
    mCDSTailer = new CDSTailer(cds_tailer_connection, mPollMaxTimeToWait,
        mBarrier);

    Ndb *part_tailer_connection = create_ndb_connection(mHiveMetaDatabaseName);
    mPARTTailer = new PARTTailer(part_tailer_connection, mPollMaxTimeToWait,
                               mBarrier);

    Ndb *idxs_tailer_connection = create_ndb_connection(mHiveMetaDatabaseName);
    mIDXSTailer = new IDXSTailer(idxs_tailer_connection, mPollMaxTimeToWait,
                                 mBarrier);

    Ndb *skl_tailer_connection = create_ndb_connection(mHiveMetaDatabaseName);
    mSkewedLocTailer = new SkewedLocTailer(skl_tailer_connection,
        mPollMaxTimeToWait, mBarrier);

    Ndb *skv_tailer_connection = create_ndb_connection(mHiveMetaDatabaseName);
    mSkewedValuesTailer = new SkewedValuesTailer(skv_tailer_connection,
        mPollMaxTimeToWait, mBarrier);
  }

  if(mStats) {
    std::vector<MetricsProvider*> providers;
    if(mMutationsTU.isEnabled()){
      providers.push_back(mProjectsElasticSearch);
    }
    if(mFileProvenanceTU.isEnabled()){
      providers.push_back(mFileProvenanceElastic);
    }
    if(mAppProvenanceTU.isEnabled()){
      providers.push_back(mAppProvenanceElastic);
    }
    mMetricsProviders = new MetricsProviders(providers);
    mHttpServer = new HttpServer(mMetricsServer, *mMetricsProviders);
  }
}

Notifier::~Notifier() {
  delete mFsMutationsTableTailer;
  delete mFsMutationsDataReaders;
  delete mFsMutationsBatcher;
  delete mMetadataLogTailer;
  delete mSchemabasedMetadataReaders;
  delete mSchemabasedMetadataBatcher;
  ndb_end(2);
}
