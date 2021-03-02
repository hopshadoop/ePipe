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

#include "FileProvenanceElasticDataReader.h"

FileProvenanceElasticDataReader::FileProvenanceElasticDataReader(SConn hopsConn, const bool hopsworks,
        int file_lru_cap, int xattr_lru_cap, int inodes_lru_cap, const std::string ml_index)
: NdbDataReader(hopsConn, hopsworks), mFileLogTable(file_lru_cap, xattr_lru_cap), inodesTable(inodes_lru_cap), mMLIndex(ml_index)  {
}

class ElasticHelper {
public:

  static std::string aliveState(std::string id, std::string index, FileProvenanceRow row, std::string mlId, FileProvenanceConstants::MLType mlType) {
    rapidjson::Document op;
    op.SetObject();
    rapidjson::Document::AllocatorType& opAlloc = op.GetAllocator();

    rapidjson::Value opVal(rapidjson::kObjectType);
    opVal.AddMember("_id", rapidjson::Value().SetString(id.c_str(), opAlloc), opAlloc);
    opVal.AddMember("_type", rapidjson::Value().SetString("_doc", opAlloc), opAlloc);
    opVal.AddMember("_index", rapidjson::Value().SetString(index.c_str(), opAlloc), opAlloc);
    op.AddMember("update", opVal, opAlloc);

    rapidjson::Document data;
    data.SetObject();
    rapidjson::Document::AllocatorType& dataAlloc = data.GetAllocator();

    rapidjson::Value dataVal(rapidjson::kObjectType);

    dataVal.AddMember("inode_id",         rapidjson::Value().SetInt64(row.mInodeId), dataAlloc);
    dataVal.AddMember("create_timestamp", rapidjson::Value().SetInt64(row.mTimestamp), dataAlloc);
    dataVal.AddMember("app_id",           rapidjson::Value().SetString(row.mAppId.c_str(), dataAlloc), dataAlloc);
    dataVal.AddMember("user_id",          rapidjson::Value().SetInt(row.mUserId), dataAlloc);
    dataVal.AddMember("project_i_id",     rapidjson::Value().SetInt64(row.mProjectId), dataAlloc);
    dataVal.AddMember("dataset_i_id",     rapidjson::Value().SetInt64(row.mDatasetId), dataAlloc);
    dataVal.AddMember("parent_i_id",      rapidjson::Value().SetInt64(row.mParentId), dataAlloc);
    dataVal.AddMember("inode_name",       rapidjson::Value().SetString(row.mInodeName.c_str(), dataAlloc), dataAlloc);
    dataVal.AddMember("project_name",     rapidjson::Value().SetString(row.mProjectName.c_str(), dataAlloc), dataAlloc);
    dataVal.AddMember("ml_id",            rapidjson::Value().SetString(mlId.c_str(), dataAlloc), dataAlloc);
    dataVal.AddMember("ml_type",          rapidjson::Value().SetString(FileProvenanceConstants::MLTypeToStr(mlType).c_str(), dataAlloc), dataAlloc);
    dataVal.AddMember("partition_id",     rapidjson::Value().SetInt64(row.mPartitionId), dataAlloc);
    dataVal.AddMember("r_create_timestamp",    rapidjson::Value().SetString(readable_timestamp(row.mTimestamp).c_str(), dataAlloc), dataAlloc);
    
    data.AddMember("doc", dataVal, dataAlloc);
    data.AddMember("doc_as_upsert", rapidjson::Value().SetBool(true), dataAlloc);

    rapidjson::StringBuffer opBuffer;
    rapidjson::Writer<rapidjson::StringBuffer> opWriter(opBuffer);
    op.Accept(opWriter);

    rapidjson::StringBuffer dataBuffer;
    rapidjson::Writer<rapidjson::StringBuffer> dataWriter(dataBuffer);
    data.Accept(dataWriter);
    
    std::stringstream out;
    out << opBuffer.GetString() << std::endl << dataBuffer.GetString();
    LOG_TRACE("file prov - elastic - add view" << out.str());
    return out.str();
  }

  static std::string addProjectIIdToState(std::string id, std::string index, FileProvenanceRow row) {
    rapidjson::Document op;
    op.SetObject();
    rapidjson::Document::AllocatorType& opAlloc = op.GetAllocator();

    rapidjson::Value opVal(rapidjson::kObjectType);
    opVal.AddMember("_id", rapidjson::Value().SetString(id.c_str(), opAlloc), opAlloc);
    opVal.AddMember("_type", rapidjson::Value().SetString("_doc", opAlloc), opAlloc);
    opVal.AddMember("_index", rapidjson::Value().SetString(index.c_str(), opAlloc), opAlloc);
    op.AddMember("update", opVal, opAlloc);

    rapidjson::Document data;
    data.SetObject();
    rapidjson::Document::AllocatorType& dataAlloc = data.GetAllocator();

    rapidjson::Value dataVal(rapidjson::kObjectType);

    dataVal.AddMember("project_i_id",     rapidjson::Value().SetInt64(row.mProjectId), dataAlloc);

    data.AddMember("doc", dataVal, dataAlloc);
    data.AddMember("doc_as_upsert", rapidjson::Value().SetBool(true), dataAlloc);

    rapidjson::StringBuffer opBuffer;
    rapidjson::Writer<rapidjson::StringBuffer> opWriter(opBuffer);
    op.Accept(opWriter);

    rapidjson::StringBuffer dataBuffer;
    rapidjson::Writer<rapidjson::StringBuffer> dataWriter(dataBuffer);
    data.Accept(dataWriter);

    std::stringstream out;
    out << opBuffer.GetString() << std::endl << dataBuffer.GetString();
    LOG_TRACE("file prov - elastic - project inode id" << out.str());
    return out.str();
  }

  static std::string addXAttrToState(std::string id, std::string index, FileProvenanceRow row, std::string val) {
    //id
    rapidjson::Document cleanupId;
    cleanupId.SetObject();
    rapidjson::Document::AllocatorType& cleanupAlloc = cleanupId.GetAllocator();

    rapidjson::Value cleanupIdVal(rapidjson::kObjectType);
    cleanupIdVal.AddMember("_id", rapidjson::Value().SetString(id.c_str(), cleanupAlloc), cleanupAlloc);
    cleanupIdVal.AddMember("_type", rapidjson::Value().SetString("_doc", cleanupAlloc), cleanupAlloc);
    cleanupIdVal.AddMember("_index", rapidjson::Value().SetString(index.c_str(), cleanupAlloc), cleanupAlloc);

    cleanupId.AddMember("update", cleanupIdVal, cleanupId.GetAllocator());

    //clean previous
    rapidjson::Document cleanup;
    cleanup.SetObject();

    std::stringstream script;
    script << "if(ctx._source.containsKey(\"" << FileProvenanceConstants::XATTR << "\")){ ";
    script << "if(ctx._source." << FileProvenanceConstants::XATTR << ".containsKey(\"" << row.mXAttrName << "\")){ ";
    script << "ctx._source." << FileProvenanceConstants::XATTR << ".remove(\"" << row.mXAttrName <<  "\");";
    script << "} else{ ctx.op=\"noop\";}";
    script << "} else{ ctx.op=\"noop\";}";

    rapidjson::Value scriptVal(script.str().c_str(), cleanup.GetAllocator());
    cleanup.AddMember("scripted_upsert", rapidjson::Value().SetBool(true), cleanup.GetAllocator());
    cleanup.AddMember("script", scriptVal, cleanup.GetAllocator());
    rapidjson::Document emptyUpsert;
    emptyUpsert.SetObject();
    cleanup.AddMember("upsert", emptyUpsert, cleanup.GetAllocator());

    //id
    rapidjson::Document op;
    op.SetObject();
    rapidjson::Document::AllocatorType& opAlloc = op.GetAllocator();

    rapidjson::Value opVal(rapidjson::kObjectType);
    opVal.AddMember("_id", rapidjson::Value().SetString(id.c_str(), opAlloc), opAlloc);
    opVal.AddMember("_type", rapidjson::Value().SetString("_doc", opAlloc), opAlloc);
    opVal.AddMember("_index", rapidjson::Value().SetString(index.c_str(), opAlloc), opAlloc);
    op.AddMember("update", opVal, opAlloc);

    //update
    rapidjson::Document data;
    data.SetObject();
    rapidjson::Document::AllocatorType& dataAlloc = data.GetAllocator();

    rapidjson::Value dataVal(rapidjson::kObjectType);
    rapidjson::Value dataValAux(rapidjson::kObjectType);

    rapidjson::Value provXAttr(FileProvenanceConstants::XATTR.c_str(), dataAlloc);
    rapidjson::Value xattrKey(row.mXAttrName.c_str(), dataAlloc);
    rapidjson::Value xattr(rapidjson::kObjectType);
    rapidjson::Value xAttrAux1(val.c_str(), dataAlloc);
    xattr.AddMember("raw", xAttrAux1, dataAlloc);

    rapidjson::Document xattrJson(&data.GetAllocator()); 
    if(!xattrJson.Parse(val.c_str()).HasParseError()) {     
      xattr.AddMember("value", xattrJson.Move(), dataAlloc);
    } else {
      rapidjson::Value xAttrAux2(val.c_str(), dataAlloc);
      xattr.AddMember("value", xAttrAux2, dataAlloc);
    }
    dataValAux.AddMember(xattrKey, xattr, dataAlloc);
    dataVal.AddMember(provXAttr, dataValAux, dataAlloc);
    data.AddMember("doc", dataVal, dataAlloc);
    data.AddMember("doc_as_upsert", rapidjson::Value().SetBool(true), dataAlloc);
    //done
    rapidjson::StringBuffer cleanupIdBuffer;
    rapidjson::Writer<rapidjson::StringBuffer> cleanupIdWriter(cleanupIdBuffer);
    cleanupId.Accept(cleanupIdWriter);

    rapidjson::StringBuffer cleanupBuffer;
    rapidjson::Writer<rapidjson::StringBuffer> cleanupWriter(cleanupBuffer);
    cleanup.Accept(cleanupWriter);

    rapidjson::StringBuffer opBuffer;
    rapidjson::Writer<rapidjson::StringBuffer> opWriter(opBuffer);
    op.Accept(opWriter);

    rapidjson::StringBuffer dataBuffer;
    rapidjson::Writer<rapidjson::StringBuffer> dataWriter(dataBuffer);
    data.Accept(dataWriter);
    
    std::stringstream out;
    out << cleanupIdBuffer.GetString()  << std::endl 
        << cleanupBuffer.GetString()    << std::endl 
        << opBuffer.GetString()         << std::endl 
        << dataBuffer.GetString();
    LOG_TRACE("file prov - elastic - update xattr" << out.str());
    return out.str();
  }

  static std::string deleteXAttrFromState(std::string id, std::string index, FileProvenanceRow row) {
    //id
    rapidjson::Document cleanupId;
    cleanupId.SetObject();
    rapidjson::Document::AllocatorType& cleanupAlloc = cleanupId.GetAllocator();

    rapidjson::Value cleanupIdVal(rapidjson::kObjectType);
    cleanupIdVal.AddMember("_id", rapidjson::Value().SetString(id.c_str(), cleanupAlloc), cleanupAlloc);
    cleanupIdVal.AddMember("_type", rapidjson::Value().SetString("_doc", cleanupAlloc), cleanupAlloc);
    cleanupIdVal.AddMember("_index", rapidjson::Value().SetString(index.c_str(), cleanupAlloc), cleanupAlloc);
    cleanupId.AddMember("update", cleanupIdVal, cleanupAlloc);

    //cleanup
    rapidjson::Document cleanup;
    cleanup.SetObject();

    std::stringstream script;
    script << "if(ctx._source.containsKey(\"" << FileProvenanceConstants::XATTR << "\")){ ";
    script << "if(ctx._source." << FileProvenanceConstants::XATTR << ".containsKey(\"" << row.mXAttrName << "\")){ ";
    script << "ctx._source." << FileProvenanceConstants::XATTR << ".remove(\"" << row.mXAttrName <<  "\");";
    script << "} else{ ctx.op=\"noop\";}";
    script << "} else{ ctx.op=\"noop\";}";

    rapidjson::Value scriptVal(script.str().c_str(), cleanup.GetAllocator());
    cleanup.AddMember("script", scriptVal, cleanup.GetAllocator());

    //done
    rapidjson::StringBuffer cleanupIdBuffer;
    rapidjson::Writer<rapidjson::StringBuffer> cleanupIdWriter(cleanupIdBuffer);
    cleanupId.Accept(cleanupIdWriter);

    rapidjson::StringBuffer cleanupBuffer;
    rapidjson::Writer<rapidjson::StringBuffer> cleanupWriter(cleanupBuffer);
    cleanup.Accept(cleanupWriter);
    
    std::stringstream out;
    out << cleanupIdBuffer.GetString() << std::endl 
        << cleanupBuffer.GetString();
    LOG_TRACE("file prov - elastic - delete xattr" << out.str());
    return out.str();
  }

  static std::string deadState(std::string id, std::string index) {

    rapidjson::Document op;
    op.SetObject();
    rapidjson::Document::AllocatorType& opAlloc = op.GetAllocator();

    rapidjson::Value opVal(rapidjson::kObjectType);

    opVal.AddMember("_id", rapidjson::Value().SetString(id.c_str(), opAlloc), opAlloc);
    opVal.AddMember("_type", rapidjson::Value().SetString("_doc", opAlloc), opAlloc);
    opVal.AddMember("_index", rapidjson::Value().SetString(index.c_str(), opAlloc), opAlloc);

    op.AddMember("delete", opVal, opAlloc);

    rapidjson::StringBuffer reqBuffer;
    rapidjson::Writer<rapidjson::StringBuffer> reqWriter(reqBuffer);
    op.Accept(reqWriter);
    
    std::stringstream out;
    out << reqBuffer.GetString();
    LOG_TRACE("file prov - elastic - delete view" << out.str());
    return out.str();
  }

  static std::string fileOp(std::string id, std::string index, FileProvenanceRow row, std::string mlId, FileProvenanceConstants::MLType mlType) {
    rapidjson::Document op;
    op.SetObject();
    rapidjson::Document::AllocatorType& opAlloc = op.GetAllocator();

    rapidjson::Value opVal(rapidjson::kObjectType);
    opVal.AddMember("_id", rapidjson::Value().SetString(id.c_str(), opAlloc), opAlloc);
    opVal.AddMember("_type", rapidjson::Value().SetString("_doc", opAlloc), opAlloc);
    opVal.AddMember("_index", rapidjson::Value().SetString(index.c_str(), opAlloc), opAlloc);

    op.AddMember("update", opVal, opAlloc);

    rapidjson::Document data;
    data.SetObject();
    rapidjson::Document::AllocatorType& dataAlloc = data.GetAllocator();

    rapidjson::Value dataVal(rapidjson::kObjectType);

    dataVal.AddMember("inode_id",         rapidjson::Value().SetInt64(row.mInodeId), dataAlloc);
    dataVal.AddMember("inode_operation",  rapidjson::Value().SetString(row.mOperation.c_str(), dataAlloc), dataAlloc);
    dataVal.AddMember("logical_time",     rapidjson::Value().SetInt(row.mLogicalTime), dataAlloc);
    dataVal.AddMember("timestamp",        rapidjson::Value().SetInt64(row.mTimestamp), dataAlloc);
    dataVal.AddMember("app_id",           rapidjson::Value().SetString(row.mAppId.c_str(), dataAlloc), dataAlloc);
    dataVal.AddMember("user_id",           rapidjson::Value().SetInt(row.mUserId), dataAlloc);
    dataVal.AddMember("project_i_id",     rapidjson::Value().SetInt64(row.mProjectId), dataAlloc);
    dataVal.AddMember("dataset_i_id",     rapidjson::Value().SetInt64(row.mDatasetId), dataAlloc);
    dataVal.AddMember("parent_i_id",      rapidjson::Value().SetInt64(row.mParentId), dataAlloc);
    dataVal.AddMember("inode_name",       rapidjson::Value().SetString(row.mInodeName.c_str(), dataAlloc), dataAlloc);
    dataVal.AddMember("project_name",     rapidjson::Value().SetString(row.mProjectName.c_str(), dataAlloc), dataAlloc);
    dataVal.AddMember("ml_id",            rapidjson::Value().SetString(mlId.c_str(), dataAlloc), dataAlloc);
    dataVal.AddMember("ml_type",          rapidjson::Value().SetString(FileProvenanceConstants::MLTypeToStr(mlType).c_str(), dataAlloc), dataAlloc);
    dataVal.AddMember("entry_type",       rapidjson::Value().SetString("operation", dataAlloc), dataAlloc);
    dataVal.AddMember("partition_id",     rapidjson::Value().SetInt64(row.mPartitionId), dataAlloc);
    dataVal.AddMember("r_timestamp",      rapidjson::Value().SetString(readable_timestamp(row.mTimestamp).c_str(), dataAlloc), dataAlloc);

    data.AddMember("doc", dataVal, dataAlloc);
    data.AddMember("doc_as_upsert", rapidjson::Value().SetBool(true), dataAlloc);

    rapidjson::StringBuffer opBuffer;
    rapidjson::Writer<rapidjson::StringBuffer> opWriter(opBuffer);
    op.Accept(opWriter);

    rapidjson::StringBuffer dataBuffer;
    rapidjson::Writer<rapidjson::StringBuffer> dataWriter(dataBuffer);
    data.Accept(dataWriter);
    
    std::stringstream out;
    out << opBuffer.GetString() << std::endl << dataBuffer.GetString();
    LOG_TRACE("file prov - elastic - file op" << out.str());
    return out.str();
  }

  static std::string addXAttrOp(std::string id, std::string index, FileProvenanceRow row, std::string val, std::string mlId, FileProvenanceConstants::MLType mlType) {
    rapidjson::Document op;
    op.SetObject();
    rapidjson::Document::AllocatorType& opAlloc = op.GetAllocator();

    rapidjson::Value opVal(rapidjson::kObjectType);
    opVal.AddMember("_id", rapidjson::Value().SetString(id.c_str(), opAlloc), opAlloc);
    opVal.AddMember("_type", rapidjson::Value().SetString("_doc", opAlloc), opAlloc);
    opVal.AddMember("_index", rapidjson::Value().SetString(index.c_str(), opAlloc), opAlloc);

    op.AddMember("update", opVal, opAlloc);

    rapidjson::Document data;
    data.SetObject();
    rapidjson::Document::AllocatorType& dataAlloc = data.GetAllocator();

    rapidjson::Value dataVal(rapidjson::kObjectType);
    rapidjson::Value dataValAux(rapidjson::kObjectType);

    dataVal.AddMember("inode_id",         rapidjson::Value().SetInt64(row.mInodeId), dataAlloc);
    dataVal.AddMember("inode_operation",  rapidjson::Value().SetString(row.mOperation.c_str(), dataAlloc), dataAlloc);
    dataVal.AddMember("logical_time",     rapidjson::Value().SetInt(row.mLogicalTime), dataAlloc);
    dataVal.AddMember("timestamp",        rapidjson::Value().SetInt64(row.mTimestamp), dataAlloc);
    dataVal.AddMember("app_id",           rapidjson::Value().SetString(row.mAppId.c_str(), dataAlloc), dataAlloc);
    dataVal.AddMember("user_id",           rapidjson::Value().SetInt(row.mUserId), dataAlloc);
    dataVal.AddMember("project_i_id",     rapidjson::Value().SetInt64(row.mProjectId), dataAlloc);
    dataVal.AddMember("dataset_i_id",     rapidjson::Value().SetInt64(row.mDatasetId), dataAlloc);
    dataVal.AddMember("parent_i_id",      rapidjson::Value().SetInt64(row.mParentId), dataAlloc);
    dataVal.AddMember("inode_name",       rapidjson::Value().SetString(row.mInodeName.c_str(), dataAlloc), dataAlloc);
    dataVal.AddMember("project_name",     rapidjson::Value().SetString(row.mProjectName.c_str(), dataAlloc), dataAlloc);
    dataVal.AddMember("ml_id",            rapidjson::Value().SetString(mlId.c_str(), dataAlloc), dataAlloc);
    dataVal.AddMember("ml_type",          rapidjson::Value().SetString(FileProvenanceConstants::MLTypeToStr(mlType).c_str(), dataAlloc), dataAlloc);
    dataVal.AddMember("entry_type",       rapidjson::Value().SetString("operation", dataAlloc), dataAlloc);
    dataVal.AddMember("partition_id",     rapidjson::Value().SetInt64(row.mPartitionId), dataAlloc);
    dataVal.AddMember("r_timestamp",      rapidjson::Value().SetString(readable_timestamp(row.mTimestamp).c_str(), dataAlloc), dataAlloc);

    rapidjson::Value xattr(rapidjson::kObjectType);
    xattr.AddMember("raw", rapidjson::Value().SetString(val.c_str(), dataAlloc), dataAlloc);
    rapidjson::Document xattrJson(&data.GetAllocator());
    if(!xattrJson.Parse(val.c_str()).HasParseError()) {     
      xattr.AddMember("value", xattrJson.Move(), dataAlloc);
    }
    // rapidjson::Value xattrKey(row.mXAttrName.c_str(), dataAlloc);
    rapidjson::Value provXAttr(FileProvenanceConstants::XATTR.c_str(), dataAlloc);
    rapidjson::Value xattrKey(row.mXAttrName.c_str(), dataAlloc);
    dataValAux.AddMember(xattrKey, xattr, dataAlloc);
    dataVal.AddMember(provXAttr, dataValAux, dataAlloc);
    // dataVal.AddMember(xattrKey, xattr, dataAlloc);
    data.AddMember("doc", dataVal, dataAlloc);
    data.AddMember("doc_as_upsert", rapidjson::Value().SetBool(true), dataAlloc);

    rapidjson::StringBuffer opBuffer;
    rapidjson::Writer<rapidjson::StringBuffer> opWriter(opBuffer);
    op.Accept(opWriter);

    rapidjson::StringBuffer dataBuffer;
    rapidjson::Writer<rapidjson::StringBuffer> dataWriter(dataBuffer);
    data.Accept(dataWriter);
    
    std::stringstream out;
    out << opBuffer.GetString() << std::endl << dataBuffer.GetString();
    LOG_TRACE("file prov - elastic - add xattr op" << out.str());
    return out.str();
  }

  static std::string deleteXAttrOp(std::string id, std::string index, FileProvenanceRow row, std::string mlId, FileProvenanceConstants::MLType mlType) {
    rapidjson::Document op;
    op.SetObject();
    rapidjson::Document::AllocatorType& opAlloc = op.GetAllocator();

    rapidjson::Value opVal(rapidjson::kObjectType);
    opVal.AddMember("_id", rapidjson::Value().SetString(id.c_str(), opAlloc), opAlloc);
    opVal.AddMember("_type", rapidjson::Value().SetString("_doc", opAlloc), opAlloc);
    opVal.AddMember("_index", rapidjson::Value().SetString(index.c_str(), opAlloc), opAlloc);

    op.AddMember("update", opVal, opAlloc);

    rapidjson::Document data;
    data.SetObject();
    rapidjson::Document::AllocatorType& dataAlloc = data.GetAllocator();

    rapidjson::Value dataVal(rapidjson::kObjectType);

    dataVal.AddMember("inode_id",         rapidjson::Value().SetInt64(row.mInodeId), dataAlloc);
    dataVal.AddMember("inode_operation",  rapidjson::Value().SetString(row.mOperation.c_str(), dataAlloc), dataAlloc);
    dataVal.AddMember("logical_time",     rapidjson::Value().SetInt(row.mLogicalTime), dataAlloc);
    dataVal.AddMember("timestamp",        rapidjson::Value().SetInt64(row.mTimestamp), dataAlloc);
    dataVal.AddMember("app_id",           rapidjson::Value().SetString(row.mAppId.c_str(), dataAlloc), dataAlloc);
    dataVal.AddMember("user_id",           rapidjson::Value().SetInt(row.mUserId), dataAlloc);
    dataVal.AddMember("project_i_id",     rapidjson::Value().SetInt64(row.mProjectId), dataAlloc);
    dataVal.AddMember("dataset_i_id",     rapidjson::Value().SetInt64(row.mDatasetId), dataAlloc);
    dataVal.AddMember("parent_i_id",      rapidjson::Value().SetInt64(row.mParentId), dataAlloc);
    dataVal.AddMember("inode_name",       rapidjson::Value().SetString(row.mInodeName.c_str(), dataAlloc), dataAlloc);
    dataVal.AddMember("project_name",     rapidjson::Value().SetString(row.mProjectName.c_str(), dataAlloc), dataAlloc);
    dataVal.AddMember("ml_id",            rapidjson::Value().SetString(mlId.c_str(), dataAlloc), dataAlloc);
    dataVal.AddMember("ml_type",          rapidjson::Value().SetString(FileProvenanceConstants::MLTypeToStr(mlType).c_str(), dataAlloc), dataAlloc);
    dataVal.AddMember("entry_type",       rapidjson::Value().SetString("operation", dataAlloc), dataAlloc);
    dataVal.AddMember("partition_id",     rapidjson::Value().SetInt64(row.mPartitionId), dataAlloc);
    dataVal.AddMember("r_timestamp",      rapidjson::Value().SetString(readable_timestamp(row.mTimestamp).c_str(), dataAlloc), dataAlloc);
    dataVal.AddMember("xattr_prov_key", rapidjson::Value().SetString(row.mXAttrName.c_str(), dataAlloc), dataAlloc);

    data.AddMember("doc", dataVal, dataAlloc);
    data.AddMember("doc_as_upsert", rapidjson::Value().SetBool(true), dataAlloc);

    rapidjson::StringBuffer opBuffer;
    rapidjson::Writer<rapidjson::StringBuffer> opWriter(opBuffer);
    op.Accept(opWriter);

    rapidjson::StringBuffer dataBuffer;
    rapidjson::Writer<rapidjson::StringBuffer> dataWriter(dataBuffer);
    data.Accept(dataWriter);
    
    std::stringstream out;
    out << opBuffer.GetString() << std::endl << dataBuffer.GetString();
    LOG_TRACE("file prov - elastic - delete xattr op" << out.str());
    return out.str();
  }

  static std::string readable_timestamp(Int64 timestamp) {
    using namespace boost::posix_time;
    using namespace boost::gregorian;
    time_t raw_t = (time_t)timestamp/1000; //time_t is time in seconds?
    ptime p_timestamp = from_time_t(raw_t);
    std::stringstream t_date;
    t_date << p_timestamp.date().year() << "." << p_timestamp.date().month() << "." << p_timestamp.date().day();
    std::stringstream t_time;
    t_time << p_timestamp.time_of_day().hours() << ":" << p_timestamp.time_of_day().minutes() << ":" << p_timestamp.time_of_day().seconds();
    std::stringstream readable_timestamp;
    readable_timestamp << t_date.str().c_str() << " " << t_time.str().c_str();
    return readable_timestamp.str();
  }

  static std::string opId(FileProvenanceRow row) {
    std::stringstream out;
    out << row.mInodeId << "-" << row.mOperation << "-" << row.mLogicalTime << "-" << row.mTimestamp << "-" << row.mAppId << "-" << row.mUserId;
    return out.str();
  }

  static std::string stateId(FileProvenanceRow row) {
    std::stringstream out;
    out << row.mInodeId;
    return out.str();
  }
};

void FileProvenanceElasticDataReader::processAddedandDeleted(Pq* data_batch, eBulk& bulk) {
  ULSet inodes = getViewInodes(data_batch);

  for (Pq::iterator it = data_batch->begin(); it != data_batch->end(); ++it) {
    FileProvenanceRow row = *it;
    ProcessRowResult result = process_row(row);
    LogHandler* lh = mFileLogTable.getLogHandler(result.mLogPK, result.mCompanionPK);
    if (inodes.find(row.mInodeId) != inodes.end() || result.mProvOp == FileProvenanceConstantsRaw::Operation::OP_DELETE) {
      std::string elasticBulkOps = getElasticBulkOps(result.mElasticOps);
      bulk.push(lh, row.mEventCreationTime, elasticBulkOps);
    } else {
      LOG_DEBUG("file prov - prep - op: " << row.getPK().to_string() << " hdfs inode missing file:" << row.mInodeName << "dataset:" << row.mDatasetName);
      bulk.push(lh, row.mEventCreationTime, FileProvenanceConstants::ELASTIC_NOP);
    }
  }
}

ULSet FileProvenanceElasticDataReader::getViewInodes(Pq* data_batch) {
  AnyVec anyVec;
  for (Pq::iterator it = data_batch->begin(); it != data_batch->end(); ++it) {
    FileProvenanceRow row = *it;
    std::pair<FileProvenanceConstants::MLType, std::string> mlAux = FileProvenanceConstants::parseML(row);
    if(mlAux.first != FileProvenanceConstants::MLType::NONE) {
      AnyMap pk;
      pk[0] = row.mParentId;
      pk[1] = row.mInodeName;
      pk[2] = row.mPartitionId;
      anyVec.push_back(pk);
    }
  }
  INodeVec inodesAux = inodesTable.get(mNdbConnection, anyVec);
  ULSet inodes;
  for (INodeVec::iterator it = inodesAux.begin(); it != inodesAux.end(); ++it) {
    INodeRow row = *it;
    inodes.insert(row.mId);
  }
  return inodes;
}

std::string FileProvenanceElasticDataReader::getElasticBulkOps(std::list <std::string> bulkOps) {
  std::stringstream out;
  if (bulkOps.empty()) {
    out << FileProvenanceConstants::ELASTIC_NOP;
  } else {
    for (std::string op : bulkOps) {
      out << op << std::endl;
    }
  }
  return out.str();
}

ProcessRowResult FileProvenanceElasticDataReader::process_row(FileProvenanceRow row) {
  LOG_DEBUG("file prov - processing:" << row.getPK().to_string() << " name:" << row.mInodeName << " dataset:" << row.mDatasetName);
  std::list<std::string> bulkOps;
  FileProvenanceConstantsRaw::Operation fileOp = FileProvenanceConstantsRaw::findOp(row.mOperation);
  std::pair<FileProvenanceConstants::MLType, std::string> mlAux = FileProvenanceConstants::parseML(row);
  LOG_DEBUG("file prov - ml type:" << mlAux.first << " inode:" << row.mInodeId << " name:" << row.mInodeName);
  boost::optional<FPXAttrBufferRow> datasetProvCoreRow = getProvCore(row.mDatasetId, row.mDatasetLogicalTime);
  boost::optional<FileProvenanceConstants::ProvOpStoreType> datasetProvCore = boost::make_optional(false, FileProvenanceConstants::ProvOpStoreType::STORE_NONE);
  bool skipElasticOp = false;
  std::string projectIndex;
  if(datasetProvCoreRow) {
    std::pair<FileProvenanceConstants::ProvOpStoreType, Int64> pc = FileProvenanceConstants::provCore(datasetProvCoreRow.get().mValue);
    datasetProvCore = pc.first;
    row.mProjectId = pc.second;
  } else {
    //if no dataset prov core - probably dataset was deleted and processed already - we default to logging the operation
    LOG_DEBUG("file prov - core - dataset prov core missing - iId:" << row.mInodeId << " dIId:" << row.mDatasetId << " op:" << row.mOperation << " pIId:" << row.mProjectId);
    datasetProvCore = FileProvenanceConstants::ProvOpStoreType::STORE_ALL;
  }
  if(row.mProjectId == -1) {
    //without a project id, we will not be able to log it
    LOG_WARN("file prov - no project id - skipping operation" << row.getPK().to_string() << " dataset:" << row.mDatasetName);
    skipElasticOp = true;
  } else if(!projectExists(row.mProjectId, row.mTimestamp)) {
    //without a project, there is no reason/place(index) to log it to
    LOG_DEBUG("file prov - no project inode(deleted?) - skipping operation" << row.getPK().to_string());
    skipElasticOp = true;
  } else {
    projectIndex = FileProvenanceConstants::projectIndex(row.mProjectId);
  }

  switch (fileOp) {
    case FileProvenanceConstantsRaw::Operation::OP_CREATE: {
      if(!skipElasticOp) {
        if(mlAux.first == FileProvenanceConstants::MLType::HIVE || mlAux.first == FileProvenanceConstants::MLType::FEATURE) {
          //fix project name - in hive&hops, project name is deduced from dataset name and there the project name is lowercased
          row.mProjectName = FileProvCache::getInstance().getProjectName(row.mProjectId);
        }
        switch (mlAux.first) {
          case FileProvenanceConstants::MLType::DATASET:
          case FileProvenanceConstants::MLType::HIVE:
          case FileProvenanceConstants::MLType::FEATURE:
          case FileProvenanceConstants::MLType::TRAINING_DATASET:
          case FileProvenanceConstants::MLType::EXPERIMENT:
          case FileProvenanceConstants::MLType::MODEL: {
            switch (datasetProvCore.get()) {
              case FileProvenanceConstants::STORE_NONE: break;
              case FileProvenanceConstants::STORE_STATE: {
                std::string state = ElasticHelper::aliveState(ElasticHelper::stateId(row), mMLIndex, row, mlAux.second, mlAux.first);
                bulkOps.push_back(state);
              } break;
              case FileProvenanceConstants::STORE_ALL: {
                std::string state = ElasticHelper::aliveState(ElasticHelper::stateId(row), mMLIndex, row, mlAux.second, mlAux.first);
                bulkOps.push_back(state);
                std::string op = ElasticHelper::fileOp(ElasticHelper::opId(row), projectIndex, row, mlAux.second, mlAux.first);
                bulkOps.push_back(op);
              } break;
              default: {
                LOG_WARN("file prov - unhandled prov state:" << datasetProvCore.get() << " - skipping it");
              }
            }
          } break;
          case FileProvenanceConstants::MLType::HIVE_PART:
          case FileProvenanceConstants::MLType::FEATURE_PART:
          case FileProvenanceConstants::MLType::TRAINING_DATASET_PART:
          case FileProvenanceConstants::MLType::EXPERIMENT_PART:
          case FileProvenanceConstants::MLType::MODEL_PART: {
            if (datasetProvCore.get() == FileProvenanceConstants::STORE_ALL) {
              std::string op = ElasticHelper::fileOp(ElasticHelper::opId(row), projectIndex, row, mlAux.second, mlAux.first);
              bulkOps.push_back(op);
            }
          } break;
          default: {
            LOG_WARN("file prov - unhandled artifact type:" << mlAux.first << " - skipping it");
          }
        }
      }
      return rowResult(bulkOps, row.getPK(), boost::none, fileOp);
    } break;
    case FileProvenanceConstantsRaw::Operation::OP_DELETE: {
      if(!skipElasticOp) {
        switch (mlAux.first) {
          case FileProvenanceConstants::MLType::DATASET:
          case FileProvenanceConstants::MLType::HIVE:
          case FileProvenanceConstants::MLType::FEATURE:
          case FileProvenanceConstants::MLType::TRAINING_DATASET:
          case FileProvenanceConstants::MLType::EXPERIMENT:
          case FileProvenanceConstants::MLType::MODEL: {
            switch (datasetProvCore.get()) {
              case FileProvenanceConstants::STORE_NONE: break;
              case FileProvenanceConstants::STORE_STATE: {
                std::string state = ElasticHelper::deadState(ElasticHelper::stateId(row), mMLIndex);
                bulkOps.push_back(state);
              } break;
              case FileProvenanceConstants::STORE_ALL: {
                std::string state = ElasticHelper::deadState(ElasticHelper::stateId(row), mMLIndex);
                bulkOps.push_back(state);
                std::string op = ElasticHelper::fileOp(ElasticHelper::opId(row), projectIndex, row, mlAux.second, mlAux.first);
                bulkOps.push_back(op);
              } break;
              default: {
                LOG_WARN("file prov - unhandled prov state:" << datasetProvCore.get() << " - skipping it");
              }
            }
          } break;
          case FileProvenanceConstants::MLType::HIVE_PART:
          case FileProvenanceConstants::MLType::FEATURE_PART:
          case FileProvenanceConstants::MLType::TRAINING_DATASET_PART:
          case FileProvenanceConstants::MLType::EXPERIMENT_PART:
          case FileProvenanceConstants::MLType::MODEL_PART: {
            if (datasetProvCore.get() == FileProvenanceConstants::STORE_ALL) {
              std::string op = ElasticHelper::fileOp(ElasticHelper::opId(row), projectIndex, row, mlAux.second, mlAux.first);
              bulkOps.push_back(op);
            }
          } break;
          default: {
            LOG_WARN("file prov - unhandled artifact type:" << mlAux.first << " - skipping it");
          }
        }
      }
      if (row.mInodeId == row.mDatasetId) {
        return rowResult(bulkOps, row.getPK(), datasetProvCoreRow.get().getPK(), fileOp);
      } else {
        return rowResult(bulkOps, row.getPK(), boost::none, fileOp);
      }
    } break;
    case FileProvenanceConstantsRaw::Operation::OP_MODIFY_DATA:
    case FileProvenanceConstantsRaw::Operation::OP_ACCESS_DATA: {
      if(!skipElasticOp) {
        switch (mlAux.first) {
          case FileProvenanceConstants::MLType::DATASET:
          case FileProvenanceConstants::MLType::HIVE:
          case FileProvenanceConstants::MLType::FEATURE:
          case FileProvenanceConstants::MLType::TRAINING_DATASET:
          case FileProvenanceConstants::MLType::EXPERIMENT:
          case FileProvenanceConstants::MLType::MODEL: {
            switch (datasetProvCore.get()) {
              case FileProvenanceConstants::STORE_NONE: break;
              case FileProvenanceConstants::STORE_STATE: break;
              case FileProvenanceConstants::STORE_ALL: {
                std::string op = ElasticHelper::fileOp(ElasticHelper::opId(row), projectIndex, row, mlAux.second, mlAux.first);
                bulkOps.push_back(op);
              } break;
              default: {
                LOG_WARN("file prov - unhandled prov state:" << datasetProvCore.get() << " - skipping it");
              }
            }
          } break;
          case FileProvenanceConstants::MLType::HIVE_PART:
          case FileProvenanceConstants::MLType::FEATURE_PART:
          case FileProvenanceConstants::MLType::TRAINING_DATASET_PART:
          case FileProvenanceConstants::MLType::EXPERIMENT_PART:
          case FileProvenanceConstants::MLType::MODEL_PART: {
            if (datasetProvCore.get() == FileProvenanceConstants::STORE_ALL) {
              std::string op = ElasticHelper::fileOp(ElasticHelper::opId(row), projectIndex, row, mlAux.second, mlAux.first);
              bulkOps.push_back(op);
            }
          } break;
          default: {
            LOG_WARN("file prov - unhandled artifact type:" << mlAux.first << " - skipping it");
          }
        }
      }
      return rowResult(bulkOps, row.getPK(), boost::none, fileOp);
    } break;
    case FileProvenanceConstantsRaw::Operation::OP_XATTR_ADD:
    case FileProvenanceConstantsRaw::Operation::OP_XATTR_UPDATE: {
      FPXAttrBufferPK xattrBufferKey = row.getXAttrBufferPK();
      boost::optional<FPXAttrBufferRow> xattr = mFileLogTable.getCompanionRow(mNdbConnection, xattrBufferKey);
      if(xattr) {
        LOG_DEBUG("file prov - processing xattr:" << xattr.get().getPK().to_string());
        if (row.mXAttrName == FileProvenanceConstantsRaw::XATTR_PROV_CORE) {
          std::pair<FileProvenanceConstants::ProvOpStoreType, Int64> opProvCore = FileProvenanceConstants::provCore(xattr.get().mValue);
          datasetProvCore = opProvCore.first;
          if (row.mProjectId == -1) {
            //if we didn't know the project until here, this new prov core can tell us the project id
            row.mProjectId = opProvCore.second;
            projectIndex = FileProvenanceConstants::projectIndex(row.mProjectId);
          }
          if (projectExists(row.mProjectId, row.mTimestamp)) {
            skipElasticOp = false;
          }
          if (!skipElasticOp) {
            switch (datasetProvCore.get()) {
              case FileProvenanceConstants::ProvOpStoreType::STORE_NONE: {
                std::string state = ElasticHelper::deadState(ElasticHelper::stateId(row), mMLIndex);
                bulkOps.push_back(state);
              } break;
              case FileProvenanceConstants::ProvOpStoreType::STORE_STATE:
              case FileProvenanceConstants::ProvOpStoreType::STORE_ALL: {
                std::string state = ElasticHelper::aliveState(ElasticHelper::stateId(row), mMLIndex, row, mlAux.second, mlAux.first);
                bulkOps.push_back(state);
              } break;
              default: {
                LOG_WARN("file prov - unhandled prov state:" << datasetProvCore.get() << " - skipping it");
              }
            }
          }
        }
      } else {
        skipElasticOp = true;
      }
      if(!skipElasticOp) {
        switch (mlAux.first) {
          case FileProvenanceConstants::MLType::FEATURE:
          case FileProvenanceConstants::MLType::TRAINING_DATASET:
          case FileProvenanceConstants::MLType::EXPERIMENT:
          case FileProvenanceConstants::MLType::MODEL:
          case FileProvenanceConstants::MLType::HIVE:
          case FileProvenanceConstants::MLType::DATASET: {
            switch (datasetProvCore.get()) {
              case FileProvenanceConstants::ProvOpStoreType::STORE_STATE: {
                if (row.mXAttrName == FileProvenanceConstants::XATTR_PROJECT_IID) {
                  std::string projIIdStateVal = ElasticHelper::addProjectIIdToState(ElasticHelper::stateId(row), mMLIndex, row);
                  bulkOps.push_back(projIIdStateVal);
                }
                std::string xattrStateVal = ElasticHelper::addXAttrToState(ElasticHelper::stateId(row), mMLIndex, row, xattr.get().mValue);
                bulkOps.push_back(xattrStateVal);
              } break;
              case FileProvenanceConstants::ProvOpStoreType::STORE_ALL: {
                if (row.mXAttrName == FileProvenanceConstants::XATTR_PROJECT_IID) {
                  std::string projIIdStateVal = ElasticHelper::addProjectIIdToState(ElasticHelper::stateId(row), mMLIndex, row);
                  bulkOps.push_back(projIIdStateVal);
                }
                std::string xattrStateVal = ElasticHelper::addXAttrToState(ElasticHelper::stateId(row), mMLIndex, row, xattr.get().mValue);
                bulkOps.push_back(xattrStateVal);
                std::string xattrOpVal = ElasticHelper::addXAttrOp(ElasticHelper::opId(row), projectIndex, row, xattr.get().mValue, mlAux.second, mlAux.first);
                bulkOps.push_back(xattrOpVal);
              } break;
              case FileProvenanceConstants::ProvOpStoreType::STORE_NONE: break;
              default: {
                LOG_WARN("file prov - unhandled prov state:" << datasetProvCore.get() << " - skipping it");
              }
            }
          } break;
          case FileProvenanceConstants::MLType::HIVE_PART:
          case FileProvenanceConstants::MLType::FEATURE_PART:
          case FileProvenanceConstants::MLType::TRAINING_DATASET_PART:
          case FileProvenanceConstants::MLType::EXPERIMENT_PART:
          case FileProvenanceConstants::MLType::MODEL_PART: {
            if (datasetProvCore.get() == FileProvenanceConstants::STORE_ALL) {
              std::string op = ElasticHelper::fileOp(ElasticHelper::opId(row), projectIndex, row, mlAux.second, mlAux.first);
              bulkOps.push_back(op);
            }
          } break;
          default: {
            LOG_WARN("file prov - unhandled artifact type:" << mlAux.first << " - skipping it");
          }
        }
      }
      if (row.mXAttrName == FileProvenanceConstantsRaw::XATTR_PROV_CORE) {
        if (datasetProvCoreRow && datasetProvCoreRow.get().getPK().mInodeLogicalTime < xattrBufferKey.mInodeLogicalTime) {
          return rowResult(bulkOps, row.getPK(), datasetProvCoreRow.get().getPK(), fileOp);
        } else {
          return rowResult(bulkOps, row.getPK(), boost::none, fileOp);
        }
      } else {
        return rowResult(bulkOps, row.getPK(), xattrBufferKey, fileOp);
      }
    } break;
    case FileProvenanceConstantsRaw::Operation::OP_XATTR_DELETE: {
      if (row.mXAttrName == FileProvenanceConstantsRaw::XATTR_PROV_CORE) {
        LOG_WARN("file prov - prov core should not be deleted, instead it should be set to NONE");
      }
      FPXAttrBufferPK xattrBufferKey = row.getXAttrBufferPK();
      if(!skipElasticOp) {
        switch (mlAux.first) {
          case FileProvenanceConstants::MLType::FEATURE:
          case FileProvenanceConstants::MLType::TRAINING_DATASET:
          case FileProvenanceConstants::MLType::EXPERIMENT:
          case FileProvenanceConstants::MLType::MODEL:
          case FileProvenanceConstants::MLType::HIVE:
          case FileProvenanceConstants::MLType::DATASET: {
            switch (datasetProvCore.get()) {
              case FileProvenanceConstants::STORE_NONE: break;
              case FileProvenanceConstants::STORE_STATE: {
                std::string xattrStateVal = ElasticHelper::deleteXAttrFromState(ElasticHelper::stateId(row), mMLIndex, row);
                bulkOps.push_back(xattrStateVal);
              } break;
              case FileProvenanceConstants::STORE_ALL: {
                std::string xattrStateVal = ElasticHelper::deleteXAttrFromState(ElasticHelper::stateId(row), mMLIndex, row);
                bulkOps.push_back(xattrStateVal);
                std::string xattrOpVal = ElasticHelper::deleteXAttrOp(ElasticHelper::opId(row), projectIndex, row, mlAux.second, mlAux.first);
                bulkOps.push_back(xattrOpVal);
              } break;
              default: {
                LOG_WARN("file prov - unhandled prov state:" << datasetProvCore.get() << " - skipping it");
              }
            }
          } break;
          case FileProvenanceConstants::MLType::HIVE_PART:
          case FileProvenanceConstants::MLType::FEATURE_PART:
          case FileProvenanceConstants::MLType::TRAINING_DATASET_PART:
          case FileProvenanceConstants::MLType::EXPERIMENT_PART:
          case FileProvenanceConstants::MLType::MODEL_PART: {
            if (datasetProvCore.get() == FileProvenanceConstants::STORE_ALL) {
              std::string op = ElasticHelper::fileOp(ElasticHelper::opId(row), projectIndex, row, mlAux.second, mlAux.first);
              bulkOps.push_back(op);
            }
          } break;
          default: {
            LOG_WARN("file prov - unhandled artifact type:" << mlAux.first << " - skipping it");
          }
        }
      }
      return rowResult(bulkOps, row.getPK(), xattrBufferKey, fileOp);
    } break;
    default: {
      LOG_WARN("file prov - operation not implemented:" << row.mOperation);
      return rowResult(bulkOps, row.getPK(), boost::none, fileOp);
    }
  }
}

bool FileProvenanceElasticDataReader::projectExists(Int64 projectIId, Int64 timestamp) {
  LOG_DEBUG("file prov - project exists check - inode:" << projectIId);
  if(FileProvCache::getInstance().projectExists(projectIId, timestamp)) {
    LOG_DEBUG("file prov - project exists - from cache");
    return true;
  } else {
    INodeRow inode = inodesTable.getByInodeId(mNdbConnection, projectIId);
    if(inode.mId == projectIId) {
      LOG_DEBUG("file prov - project exists - refresh cache");
      FileProvCache::getInstance().addProjectExists(projectIId, inode.mName, timestamp);
      return true;
    } else {
      LOG_DEBUG("file prov - project exists - deleted");
      return false;
    }
  }
}

ProcessRowResult FileProvenanceElasticDataReader::rowResult(std::list<std::string> elasticOps, FileProvenancePK logPK,
                                  boost::optional<FPXAttrBufferPK> companionPK, FileProvenanceConstantsRaw::Operation provOp) {
  ProcessRowResult result;
  result.mElasticOps = elasticOps;
  result.mLogPK = logPK;
  result.mCompanionPK = companionPK;
  result.mProvOp = provOp;
  return result;
}

boost::optional<FPXAttrBufferRow> FileProvenanceElasticDataReader::getProvCore(Int64 inodeId, int opLogicalTime) {
  boost::optional<FPXAttrBufferRow> provCore = FProvCoreCache::getInstance().get(inodeId, opLogicalTime);
  if(provCore) {
    LOG_DEBUG("file prov - core - hit cache inode:" << inodeId << ", op:" << opLogicalTime << " prov:" << provCore.get().mInodeLogicalTime);
    return provCore;
  } else {
    int fromLogicalTime = FProvCoreCache::getInstance().getProvCoreLogicalTime(inodeId, opLogicalTime);
    LOG_DEBUG("file prov - core - scanning buffer table inode:" << inodeId << ", from:" << fromLogicalTime << ", to:" << opLogicalTime);
    provCore = readProvCore(inodeId, opLogicalTime, fromLogicalTime);
    if(provCore) {
      LOG_DEBUG("file prov - core - add to cache inode:" << inodeId << ", op:" << opLogicalTime << " prov:" << provCore.get().mInodeLogicalTime);
      FProvCoreCache::getInstance().add(provCore.get(), opLogicalTime);
      return provCore;
    } else {
      return boost::none;
    }
  }
}

boost::optional<FPXAttrBufferRow> FileProvenanceElasticDataReader::readProvCore(Int64 inodeId, int opLogicalTime, int fromLogicalTime) {
  std::map<int, boost::optional<FPXAttrBufferRow>> provCoreVersions = mFileLogTable.getProvCore(mNdbConnection, inodeId, fromLogicalTime, opLogicalTime);
  LOG_DEBUG("file prov - core - inode:" << inodeId << ", from:" << fromLogicalTime << ", to:" << opLogicalTime << " found:" << provCoreVersions.size());
  if(provCoreVersions.empty()) {
    LOG_WARN("file prov - core - none found for inode:" << inodeId << ", from:" << fromLogicalTime << ", to:" << opLogicalTime);
    return boost::none;
  }
  if(provCoreVersions.rbegin()->second) {
    return provCoreVersions.rbegin()->second.get();
  } else {
    LOG_ERROR("file prov - prov core multi part - partially deleted - inode id:" << inodeId << " logical time:" << provCoreVersions.rbegin()->first);
    LOG_FATAL("file prov - prov core ndb - cannot recover");
    return boost::none;
  }
}

FileProvenanceElasticDataReader::~FileProvenanceElasticDataReader() {
}