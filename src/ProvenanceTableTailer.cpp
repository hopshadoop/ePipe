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

#include "ProvenanceTableTailer.h"

ProvenanceTableTailer::ProvenanceTableTailer(Ndb *ndb, Ndb* ndbRecovery, const
int poll_maxTimeToWait, const Barrier barrier)
: RCTableTailer(ndb, ndbRecovery, new ProvenanceLogTable(), poll_maxTimeToWait,
    barrier) {
  mQueue = new CPRq();
  mCurrentPriorityQueue = new PRpq();
}

void ProvenanceTableTailer::handleEvent(NdbDictionary::Event::TableEvent eventType, ProvenanceRow pre,
        ProvenanceRow row) {
  mLock.lock();
  mCurrentPriorityQueue->push(row);
  int size = mCurrentPriorityQueue->size();
  mLock.unlock();

  LOG_TRACE("push provenance log for [" << row.mInodeName << "] to queue[" << size << "], Op [" << row.mOperation << "]");

}

void ProvenanceTableTailer::barrierChanged() {
  PRpq* pq = NULL;
  mLock.lock();
  if (!mCurrentPriorityQueue->empty()) {
    pq = mCurrentPriorityQueue;
    mCurrentPriorityQueue = new PRpq();
  }
  mLock.unlock();

  if (pq != NULL) {
    LOG_TRACE("--------------------------------------NEW BARRIER (" << pq->size() << " events )------------------- ");
    pushToQueue(pq);
  }
}

ProvenanceRow ProvenanceTableTailer::consume() {
  ProvenanceRow row;
  mQueue->wait_and_pop(row);
  LOG_TRACE(" pop inode [" << row.mInodeId << "] from queue \n" << row.to_string());
  return row;
}

void ProvenanceTableTailer::pushToQueue(PRpq *curr) {
  while (!curr->empty()) {
    mQueue->push(curr->top());
    curr->pop();
  }
  delete curr;
}

ProvenanceTableTailer::~ProvenanceTableTailer() {
  delete mQueue;
}