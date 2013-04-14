// the caching lock server implementation

#include "lock_server_cache_rsm.h"
#include <sstream>
#include <iostream>
#include <string>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "lang/verify.h"
#include "handle.h"
#include "tprintf.h"

#define _DEBUG_

  static void *
revokethread(void *x)
{
  lock_server_cache_rsm *sc = (lock_server_cache_rsm *) x;
  sc->revoker();
  return 0;
}

  static void *
retrythread(void *x)
{
  lock_server_cache_rsm *sc = (lock_server_cache_rsm *) x;
  sc->retryer();
  return 0;
}

  lock_server_cache_rsm::lock_server_cache_rsm(class rsm *_rsm) 
: rsm (_rsm)
{
  VERIFY(pthread_mutex_init(&lid_m, NULL) == 0);
  VERIFY(pthread_cond_init(&lid_cond, 0) == 0);
  VERIFY(pthread_mutex_init(&revoke_m, NULL) == 0);
  VERIFY(pthread_cond_init(&revoke_cond, 0) == 0);
  VERIFY(pthread_mutex_init(&retry_m, NULL) == 0);
  VERIFY(pthread_cond_init(&retry_cond, 0) == 0);
  pthread_t th, th1;
  int r = pthread_create(&th, NULL, &revokethread, (void *) this);
  VERIFY (r == 0);
  r = pthread_create(&th1, NULL, &retrythread, (void *) this);
  VERIFY (r == 0);

  rsm->set_state_transfer(this);
}

  void
lock_server_cache_rsm::revoker()
{

  // This method should be a continuous loop, that sends revoke
  // messages to lock holders whenever another client wants the
  // same lock
  while(1) {
    struct revoke re ;
    std::string lockOwnerID ;
    lock_protocol::xid_t lockOwner_xid;
    rlock_protocol::status ret ;
    bool flag_revoke;
    {
      ScopedLock revokem(&revoke_m);
      while(revoke_list.empty()) {
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += 3;
        int ret = pthread_cond_timedwait(&revoke_cond, &revoke_m, &ts);
        //printf("revoker: waiting for revoke list to be empty %d\n", revoke_list.empty());
        if(ret != ETIMEDOUT) {
          VERIFY(ret == 0);
        }
      }
      re = revoke_list.front();
    }
    {
      ScopedLock lidm(&lid_m);
      lockOwnerID = lockstat_[re.lid].cltID;
      lockOwner_xid = lockstat_[re.lid].xid;
      if(lockstat_[re.lid].revoke) {
        while(lockstat_[re.lid].revoke) {
          struct timespec ts;
          clock_gettime(CLOCK_REALTIME, &ts);
          ts.tv_sec += 3;
          int ret = pthread_cond_timedwait(&lid_cond, &lid_m, &ts);
          if(ret != ETIMEDOUT) {
            VERIFY(ret == 0);
          }
          //printf("revoker: waiting for cltID %s: xid %llu of lid %llu to be released\n", lockstat_[re.lid].cltID.c_str(), lockstat_[re.lid].xid, re.lid);
        }
        flag_revoke = false;
      }
      else {
        flag_revoke = true; //Revoke only if there is no existing revoke in progress
        lockstat_[re.lid].revoke = true;
      }
    }
    if(flag_revoke){
      ScopedLock revokem(&revoke_m);
      revoke_list.pop_front();  //Pop only if we will send the revoke
    }
    if(!lockOwnerID.empty() && flag_revoke) {
      ret = makeclientRPC(rlock_protocol::revoke, re.lid, lockOwnerID, lockOwner_xid);
      if(ret != rlock_protocol::OK) {
        tprintf("revoker: failed to revoke lid %llu from %s\n", re.lid, lockOwnerID.c_str());
      }
      else {
        tprintf("revoker: revoked lid %llu from %s %llu\n", re.lid, lockOwnerID.c_str(), lockOwner_xid);
      }
    }
  }
}


  void
lock_server_cache_rsm::retryer()
{

  // This method should be a continuous loop, waiting for locks
  // to be released and then sending retry messages to those who
  // are waiting for it.
  while(1) {
    lock_protocol::lockid_t releasedlid;
    struct waitingClt waitingClient;
    {
      ScopedLock retrym(&retry_m);
      while(release_lid_list.empty()) {
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += 3;
        int ret = pthread_cond_timedwait(&retry_cond, &retry_m, &ts);
        if(ret != ETIMEDOUT) {
          VERIFY(ret == 0);
        }
        tprintf("retryer: waiting for client acquire to be released\n");
      }
      releasedlid = release_lid_list.front();
      release_lid_list.pop_front();
      tprintf("retryer: lid %llu has been released\n", releasedlid);
    }
    rlock_protocol::status ret ;
    bool clientsWaiting;
    {
      ScopedLock lidm(&lid_m);
      clientsWaiting = !(lockstat_[releasedlid].waitingCltIDs.empty());
      if (clientsWaiting) {
        waitingClient = lockstat_[releasedlid].waitingCltIDs.front();
        lockstat_[releasedlid].waitingCltIDs.pop_front();
      }
      else {
        tprintf("retryer: No waiting clients for lid %llu\n", releasedlid);
      }
    }
    if(clientsWaiting) {
      ret = makeclientRPC(rlock_protocol::retry, releasedlid, waitingClient.cltID, waitingClient.xid);
      if(ret != rlock_protocol::OK) {
        ScopedLock lidm(&lid_m);
        lockstat_[releasedlid].waitingCltIDs.push_back(waitingClient); //push it back to waiting list if retry RPC call was not successful
        tprintf("retryer: failed to retry lid clt %s for lid %llu\n", waitingClient.cltID.c_str(), releasedlid);
      }
      else {
        tprintf("retryer: sent retry to clt %s for lid %llu size %d\n", waitingClient.cltID.c_str(), releasedlid, release_lid_list.size());
      }
    }
  }
}


int lock_server_cache_rsm::acquire(lock_protocol::lockid_t lid, std::string id, 
    lock_protocol::xid_t xid, int &r)
{
  lock_protocol::status ret ;
  r = nacquire;
#ifdef _DEBUG_
  tprintf("acquire lock request from clt: %s for lid %llu:%llu\n", id.c_str(), lid, xid);
#endif

  //Mutex locks are applied in same order in all threads to prevent deadlocks
  ScopedLock revokem(&revoke_m);
  ScopedLock lidm(&lid_m);
  std::map<lock_protocol::lockid_t, struct lockstat>::iterator itrl;
  itrl = lockstat_.find(lid);

  if(itrl == lockstat_.end()) { //If the given lid is new
    struct lockstat lstat;
    lstat.cltID = id;
    lstat.stats = 1;
    lstat.xid = xid;
    lstat.revoke = false;
    lockstat_.insert(std::pair<lock_protocol::lockid_t,struct lockstat>(lid,lstat));
    VERIFY(pthread_cond_signal(&lid_cond) == 0);
#ifdef _DEBUG_
    tprintf("acquire: new lock granted to clt: %s for new lid %llu\n", id.c_str(), lid);
#endif
    ret = lock_protocol::OK;
  }
  else if(itrl->second.cltID.empty() ){ //If the given lid is free
    itrl->second.stats++;
    itrl->second.cltID = id;
    itrl->second.xid = xid;
    itrl->second.revoke = false;
    VERIFY(pthread_cond_signal(&lid_cond) == 0);
#ifdef _DEBUG_
    tprintf("acquire: granted unused lock to clt: %s for lid %llu\n", id.c_str(), lid);
#endif
    std::list<struct waitingClt>::iterator itrw;
    //Check if the clt is already present in our waiting list i.e if its a new request or a duplicate request
    for(itrw = itrl->second.waitingCltIDs.begin(); itrw != itrl->second.waitingCltIDs.end();) {
      if(itrw->cltID == id) {
        tprintf("acquire: clt %s was present in waitingCltIDs for lid %llu. Removing that first from waiting list since it has got the lock (this normally happens due to pre-mature retries). Waiting list size %d\n", itrw->cltID.c_str(), lid, itrl->second.waitingCltIDs.size());
        itrw = itrl->second.waitingCltIDs.erase(itrw);
        break;
      }
      else {
        itrw++ ;
      }
    }
    tprintf("acquire: Waiting list size %d of lid %llu removal\n", itrl->second.waitingCltIDs.size(), lid);
    ret = lock_protocol::OK;
  }
  //Lock is occupied send a RETRY reply to the client
  else {
    if(id != itrl->second.cltID) {

      struct revoke r;
      r.cltID = id;
      r.lid = lid;
      revoke_list.push_back(r);
      struct waitingClt waitingClient;
      waitingClient.cltID = id;
      waitingClient.xid = xid;
      std::list<struct waitingClt>::iterator itrw;
      bool flagDuplicate = false;
      ret = lock_protocol::RETRY;

      //Check if the clt is already present in our waiting list i.e if its a new request or a duplicate request
      for(itrw = itrl->second.waitingCltIDs.begin(); itrw != itrl->second.waitingCltIDs.end(); itrw++) {
        if(itrw->cltID == id) {
          if(itrw->xid == xid) {
            VERIFY(pthread_cond_signal(&revoke_cond) == 0);
            tprintf("acquire lock (duplicate and not re-added to waiting list) cannot be granted to clt: %s for lid %llu, sending RETRY instead (currently granted to cltID %s: clt xid %llu)\n", id.c_str(), lid, itrl->second.cltID.c_str(), itrl->second.xid);
            flagDuplicate = true;
          }
          else {
            tprintf("acquire lock duplicate request with outdated xid_lock %llu xid_client %llu is ignored clt: %s for lid %llu (return NOENT)\n", itrw->xid, xid, id.c_str(), lid);
            ret = lock_protocol::NOENT;
          }
          break;
        }
      }
      if(!flagDuplicate) {
        itrl->second.waitingCltIDs.push_back(waitingClient);
#ifdef _DEBUG_
        tprintf("acquire lock cannot be granted to clt: %s for lid %llu, sending RETRY instead ((currently granted to cltID %s: clt xid %llu)\n", id.c_str(), lid, itrl->second.cltID.c_str(), itrl->second.xid);
#endif
      }
      tprintf("acquire lock sending signal to revoke lock for lid %llu\n", lid);
      VERIFY(pthread_cond_signal(&revoke_cond) == 0);
    }
    else {
      if(itrl->second.xid == xid){
        tprintf("acquire lock for clt: %s for lid: %llu with xid: %llu is ignored since its duplicate of latest xid: %llu\n", id.c_str(), lid, xid, itrl->second.xid);
        ret = lock_protocol::OK;
      }
      else {
        tprintf("acquire lock for clt: %s for lid: %llu with xid: %llu is ignored since it does not match with latest xid: %llu (return NOENT)\n", id.c_str(), lid, xid, itrl->second.xid);
        ret = lock_protocol::NOENT;
      }
    }
  }
  return ret;
}

  int 
lock_server_cache_rsm::release(lock_protocol::lockid_t lid, std::string id, 
    lock_protocol::xid_t xid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
#ifdef _DEBUG_
  tprintf("release invoked for lid %llu by clt: %s\n", lid, id.c_str());
#endif
  ScopedLock retrym(&retry_m);
  ScopedLock lidm(&lid_m);
  std::map<lock_protocol::lockid_t, struct lockstat>::iterator itrl;
  itrl = lockstat_.find(lid);

  if(itrl != lockstat_.end() && (itrl->second.cltID.compare(id) == 0) && itrl->second.xid == xid) { //If this release request is not a stale release request

    //Add to the list of released lids
    release_lid_list.push_back(lid);
    //Clear the lock holder
    lockstat_[lid].cltID.clear();
    //Signal the retry thread about the newly released lid
    VERIFY(pthread_cond_signal(&retry_cond) == 0);
    tprintf("release lid %llu released by clt: %s %s\n", lid, id.c_str(), itrl->second.cltID.c_str());
  }
  else {
    tprintf("release lid %llu xid %llu ignored since its outdated request called by clt: %s (lockstat_ cltID: %s, lockstat_lid: %llu))\n", lid, xid, id.c_str(), lockstat_[lid].cltID.c_str(), lockstat_[lid].xid);
  }
  return ret;
}

  std::string
lock_server_cache_rsm::marshal_state()
{
  std::ostringstream ost;
  std::string r;
  ScopedLock lidm(&lid_m);
  ost << lockstat_.size();
  ost << " ";
  std::map<lock_protocol::lockid_t, struct lockstat>::iterator itrl;
  for(itrl = lockstat_.begin(); itrl != lockstat_.end(); itrl++){
    lock_protocol::lockid_t lid = itrl->first;
    ost << lid;
    ost << " ";
    r = itrl->second.cltID;
    if(!r.empty()) {
      ost << r;
    }
    else {
      ost << "0";
    }
    ost << " ";
    lock_protocol::xid_t xid = itrl->second.xid;
    ost << xid;
    ost << " ";

    std::list<struct waitingClt> wc = itrl->second.waitingCltIDs;
    ost << wc.size();
    ost << " ";
    std::list<struct waitingClt>::iterator itr;
    for(itr = wc.begin(); itr != wc.end(); itr++) {

      std::string waitingID = itr->cltID;
      lock_protocol::xid_t xid_wait = itr->xid;
      ost << waitingID;
      ost << " ";
      ost << xid_wait;
      ost << " ";
    }

    bool revokeFlag = itrl->second.revoke;
    ost << revokeFlag;
    ost << " ";
    int stats = itrl->second.stats;
    ost << stats;
    ost << " ";
  }
  r = ost.str();
  tprintf("marshall state %s\n", r.c_str());
  return r;
}

  void
lock_server_cache_rsm::unmarshal_state(std::string state)
{
  std::istringstream ist(state, std::istringstream::in);
  std::string cltID;
  tprintf("unmarshal state %s\n", state.c_str());
  unsigned int replicationSize, waitingListSize; 
  int stats ;
  bool revokeFlag;
  std::list<struct waitingClt> wc;
  lock_protocol::lockid_t lid;
  lock_protocol::xid_t xid;

  ist >> replicationSize;
  std::map<lock_protocol::lockid_t, struct lockstat>::iterator itrl;

  ScopedLock lidm(&lid_m);
  lockstat_.clear();

  for(unsigned int i = 0; i < replicationSize; i++) {
    ist >> lid;
    ist >> cltID;
    std::string emptyStr = "0";
    if(cltID == emptyStr) {
      cltID.clear();
    }
    ist >> xid;
    ist >> waitingListSize;
    for(unsigned int j = 0; j < waitingListSize; j++) {
      struct waitingClt w;
      ist >> w.cltID;
      ist >> w.xid;
      wc.push_back(w);
    }
    ist >> revokeFlag;
    ist >> stats;

    struct lockstat ls;
    ls.cltID = cltID;
    ls.xid = xid;
    ls.waitingCltIDs = wc;
    ls.revoke = revokeFlag;
    ls.stats = stats;
    lockstat_.insert(std::pair<lock_protocol::lockid_t,struct lockstat>(lid,ls));
  }
}


  lock_protocol::status
lock_server_cache_rsm::stat(lock_protocol::lockid_t lid, int &r)
{
  std::map<lock_protocol::lockid_t, struct lockstat>::iterator itrl;
  ScopedLock lidm(&lid_m);
  itrl = lockstat_.find(lid);

  if(itrl != lockstat_.end()) { //If the given lid is new
    r = itrl->second.stats;
    printf("stat for lid %llu = %d\n", lid, r);
  }
  return lock_protocol::OK;
}

rlock_protocol::status
lock_server_cache_rsm::makeclientRPC(int rpc, lock_protocol::lockid_t lid, std::string id, lock_protocol::xid_t xid) {
  rlock_protocol::status ret;
  if(!rsm->amiprimary()) {
    tprintf("makeclientRPC is not to be called for non-primary replica\n");
    ret = lock_protocol::OK;
  }
  else {
    handle h(id);
    rpcc *cl = h.safebind();
    if(cl) {
      int r;
      //Send RPC call to clt
      ret = cl->call(rpc, lid, xid, r);
      if(ret != lock_protocol::OK) {
        tprintf("makeclientRPC lid: %llu failed for %s\n", lid, id.c_str());
      }
    }
    else {
      tprintf("makeclientRPC lid: %llu bind handle failed for %s\n", lid, id.c_str());
    }
  }
  return ret;
}
