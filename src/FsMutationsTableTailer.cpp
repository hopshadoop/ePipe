/*
 * Copyright (C) 2016 Hops.io
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

/* 
 * File:   MutationsTableTailer.cpp
 * Author: Mahmoud Ismail<maism@kth.se>
 * 
 */

#include "FsMutationsTableTailer.h"
#include "Utils.h"

using namespace Utils::NdbC;

const string _mutation_table= "hdfs_metadata_log";
const int _mutation_noCols= 7;
const string _mutation_cols[_mutation_noCols]=
    {"dataset_id",
     "inode_id",
     "timestamp",
     "inode_partition_id",
     "inode_parent_id",
     "inode_name",
     "operation"
    };

const int _mutation_noEvents = 1; 
const NdbDictionary::Event::TableEvent _mutation_events[_mutation_noEvents] = { NdbDictionary::Event::TE_INSERT };

const WatchTable FsMutationsTableTailer::TABLE = {_mutation_table, _mutation_cols, _mutation_noCols , _mutation_events, _mutation_noEvents, _mutation_cols[2]};

//const static ptime EPOCH_TIME(boost::gregorian::date(1970,1,1)); 

FsMutationsTableTailer::FsMutationsTableTailer(Ndb* ndb, const int poll_maxTimeToWait, ProjectDatasetINodeCache* cache) : RCTableTailer(ndb, TABLE, poll_maxTimeToWait), mPDICache(cache) {
    mQueue = new Cpq();
//    mTimeTakenForEventsToArrive = 0;
//    mNumOfEvents = 0;
//    mPrintEveryNEvents = 0;
}

void FsMutationsTableTailer::handleEvent(NdbDictionary::Event::TableEvent eventType, NdbRecAttr* preValue[], NdbRecAttr* value[]){
    FsMutationRow row;
    row.mEventCreationTime = Utils::getCurrentTime();
    row.mDatasetId = value[0]->int32_value();
    row.mInodeId =  value[1]->int32_value();
    row.mTimestamp = value[2]->int64_value();
    row.mPartitionId = value[3]->int32_value();
    row.mParentId = value[4]->int32_value();
    row.mInodeName = get_string(value[5]);
    row.mOperation = static_cast<OperationType>(value[6]->int8_value());
    if (row.mOperation == Add || row.mOperation == Delete) {
        LOG_TRACE(" push inode [" << row.mInodeId << "] to queue, Op [" << row.mOperation << "]");
        mQueue->push(row);

        if (row.mOperation == Add) {
            mPDICache->addINodeToDataset(row.mInodeId, row.mDatasetId);
        } else if (row.mOperation == Delete) {
            mPDICache->removeINode(row.mInodeId);
        }
    } else {
       LOG_ERROR( "Unknown Operation [" << row.mOperation << "] for " << " INode [" << row.mInodeId << "]");
    }
    
//    ptime t = EPOCH_TIME + boost::posix_time::milliseconds(row.mTimestamp);
//    mTimeTakenForEventsToArrive += Utils::getTimeDiffInMilliseconds(t, row.mEventCreationTime);
//    mNumOfEvents++;
//    mPrintEveryNEvents++;
//    if(mPrintEveryNEvents>=10000){
//        double avgArrival = mTimeTakenForEventsToArrive / mNumOfEvents;
//        LOG_INFO("Average Arrival Time=" << avgArrival << " msec");
//        mPrintEveryNEvents = 0;
//    }
}

FsMutationRow FsMutationsTableTailer::consume(){
    FsMutationRow row;
    mQueue->wait_and_pop(row);
    LOG_TRACE(" pop inode [" << row.mInodeId << "] from queue \n" << row.to_string());
    return row;
}

void FsMutationsTableTailer::removeLogs(const NdbDictionary::Dictionary* database, NdbTransaction* transaction, Fmq* rows) {
    const NdbDictionary::Table* log_table = getTable(database, TABLE.mTableName);
    for(Fmq::iterator it=rows->begin(); it != rows->end() ; ++it){
        FsMutationRow row = *it;
        NdbOperation* op = getNdbOperation(transaction, log_table);
        
        op->deleteTuple();
        op->equal(_mutation_cols[0].c_str(), row.mDatasetId);
        op->equal(_mutation_cols[1].c_str(), row.mInodeId);
        op->equal(_mutation_cols[2].c_str(), (Int64)row.mTimestamp);
        
        LOG_TRACE("Delete log row: Dataset[" << row.mDatasetId << "], INode[" 
                << row.mInodeId << "], Timestamp[" << row.mTimestamp << "]");
    }
}

FsMutationsTableTailer::~FsMutationsTableTailer() {
    delete mQueue;
}

