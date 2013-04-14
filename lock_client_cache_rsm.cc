// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache_rsm.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <string>
#include <sys/types.h>
#include <unistd.h>
#include "tprintf.h"
#include "rsm_client.h"

#define _DEBUG_

int lock_client_cache_rsm::last_port = 0;

lock_release_flush_rsm::lock_release_flush_rsm(extent_client_cache *_ec):
  ec(_ec)
{
}

void
lock_release_flush_rsm::dorelease(lock_protocol::lockid_t lid) {
  //Lock ID is same as extent ID which is same as inum
  ec->flush(lid);
}

  static void *
releasethread(void *x)
{
  lock_client_cache_rsm *cc = (lock_client_cache_rsm *) x;
  cc->releaser();
  return NULL;
}


  static void *
revokethread(void *x)
{
  lock_client_cache_rsm *cc = (lock_client_cache_rsm *) x;
  cc->revoker();
  return NULL;
}

lock_client_cache_rsm::lock_client_cache_rsm(std::string xdst, 
    class lock_release_user_rsm *_lu)
: lock_client(xdst), lu(_lu)
{
  srand(time(NULL)^last_port);
  rlock_port = ((rand()%32000) | (0x1 << 10));
  const char *hname;
  // VERIFY(gethostname(hname, 100) == 0);
  hname = "127.0.0.1";
  std::ostringstream host;
  host << hname << ":" << rlock_port;
  id = host.str();
  last_port = rlock_port;
  // You fill this in Step Two, Lab 7
  // - Create rsmc, and use the object to do RPC 
  //   calls instead of the rpcc object of lock_client
  rsmc = new rsm_client(xdst);
  rpcs *rlsrpc = new rpcs(rlock_port);
  rlsrpc->reg(rlock_protocol::revoke, this, &lock_client_cache_rsm::revoke_handler);
  rlsrpc->reg(rlock_protocol::retry, this, &lock_client_cache_rsm::retry_handler);
  xid = 0;
  VERIFY(pthread_mutex_init(&lid_m, NULL) == 0);
  VERIFY(pthread_mutex_init(&revoke_m, NULL) == 0);
  VERIFY(pthread_cond_init(&lid_cond, 0) == 0);
  VERIFY(pthread_cond_init(&revoke_cond, 0) == 0);
  VERIFY(pthread_cond_init(&retry_cond, 0) == 0);
  pthread_t th, th1;
  int r = pthread_create(&th, NULL, &releasethread, (void *) this);
  VERIFY (r == 0);
  r = pthread_create(&th1, NULL, &revokethread, (void *) this);
  VERIFY (r == 0);
}

  void
lock_client_cache_rsm::releaser()
{

  // This method should be a continuous loop, waiting to be notified of
  // freed locks that have been revoked by the server, so that it can
  // send a release RPC.
  while(1){
    struct lidlist ls;
    {
      ScopedLock re(&retry_m);
      while(retry_id_list.empty()) {
        //tprintf("releaser waiting for retry list to be non-empty\n");
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += 3;
        int r = pthread_cond_timedwait(&retry_cond, &retry_m, &ts);
        if(r != ETIMEDOUT) {
          VERIFY(r == 0);
        }
        //VERIFY(pthread_cond_wait(&retry_cond, &retry_m) == 0);
      }
      ls = retry_id_list.front();
      retry_id_list.pop_front();
    }
    ScopedLock lm(&lid_m);
    while(lockm_[ls.lid].status == acquiring) {
#ifdef _DEBUG_
      tprintf("releaser client %s waiting for lid %llu existing acquire to return before retrying (%lu)\n", id.c_str(), ls.lid, pthread_self());
#endif
      struct timespec ts;
      clock_gettime(CLOCK_REALTIME, &ts);
      ts.tv_sec += 3;
      int r = pthread_cond_timedwait(&lid_cond, &lid_m, &ts);
      if(r != ETIMEDOUT ) {
        VERIFY(r == 0);
      }
      //VERIFY(pthread_cond_wait(&lid_cond, &lid_m) == 0);
    }
    if(lockm_[ls.lid].xid != ls.xid) {
      tprintf("lock_client_cache_rsm: retryer %s lid %llu:%llu cannot be retried as it does not match with lock xid %llu (%lu)\n", id.c_str(), ls.lid, ls.xid, lockm_[ls.lid].xid, (unsigned long)pthread_self());
      continue;
    }
    //Only if client is neither in locked nor the free state, must it retry
    //This is the change
    if(lockm_[ls.lid].status == retry_later) {
#ifdef _DEBUG_
      tprintf("lock_client_cache_rsm: retryer client %s lid %llu:%llu can now retry %d (%lu)\n", id.c_str(), ls.lid, ls.xid, lockm_[ls.lid].status, pthread_self());
#endif
      lockm_[ls.lid].status = none;
      VERIFY(pthread_cond_broadcast(&lid_cond) == 0);
    }
  }
  return;
}

  void
lock_client_cache_rsm::revoker()
{
  while(1) {
    struct lidlist ls ;
    {
      ScopedLock rm(&revoke_m);
      while(revoke_id_list.empty()) {
        //tprintf("revoker waiting for revoke list to be non-empty\n");
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += 3;
        int r = pthread_cond_timedwait(&revoke_cond, &revoke_m, &ts);
        if(r != ETIMEDOUT ) {
          VERIFY(r == 0);
        }
        //VERIFY(pthread_cond_wait(&revoke_cond, &revoke_m) == 0);
      }
      ls = revoke_id_list.front();
      revoke_id_list.pop_front();
    }
    {
      ScopedLock lm(&lid_m);
      while(!(lockm_[ls.lid].status == free || lockm_[ls.lid].status == none)) {
        tprintf("lock_client_cache_rsm: revoker %s waiting for lid %llu  to be free or none status:%d (%lu)\n", id.c_str(), ls.lid, lockm_[ls.lid].status, (unsigned long)pthread_self());
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += 3;
        int r = pthread_cond_timedwait(&lid_cond, &lid_m, &ts);
        if(r != ETIMEDOUT) {
          VERIFY(r == 0);
        }
        //VERIFY(pthread_cond_wait(&lid_cond, &lid_m) == 0);
      }
      if(lockm_[ls.lid].xid != ls.xid) {
        tprintf("lock_client_cache_rsm: revoker %s lid %llu:%llu cannot be revoked as it does not match with lock xid %llu (%lu)\n", id.c_str(), ls.lid, ls.xid, lockm_[ls.lid].xid, (unsigned long)pthread_self());
        continue;
      }
#ifdef _DEBUG_
      tprintf("lock_client_cache_rsm: revoker for lid %llu calling release RPC %s (%lu)\n", ls.lid, id.c_str(), pthread_self());
#endif
      lockm_[ls.lid].toRevoke = true;
      //This is the change
      lockm_[ls.lid].status = releasing;
    }
    //Call release for lid
    lock_protocol::status ret = release(ls.lid);
    VERIFY(ret == lock_protocol::OK);
  }
  return;
}

  lock_protocol::status
lock_client_cache_rsm::acquire(lock_protocol::lockid_t lid)
{
  bool acquiredLock = false, flagAcquireSentOnce = false;
  lock_protocol::xid_t xid_a;
  while(!acquiredLock)
  {
    {
      ScopedLock lm(&lid_m);
      if(lockm_.find(lid) == lockm_.end()) {
        //If the server has not seen this lock before
#ifdef _DEBUG_
        tprintf("lock_client_cache_rsm: acquire lock lid %llu is unknown for client %s (%lu)\n", lid, id.c_str(), (unsigned long)pthread_self());
#endif
        struct lockstatus st;
        st.status = none;
        st.toRevoke = false;
        lockm_.insert(std::pair<lock_protocol::lockid_t, struct lockstatus> (lid, st));
      }
      else if(lockm_[lid].status == acquiring || lockm_[lid].status == locked || lockm_[lid].status == releasing) {
        while(!(lockm_[lid].status == free || lockm_[lid].status == none )) {
          tprintf("acquire %s waiting for lid %llu status %d  to be none or free(%lu)\n", id.c_str(), lid, lockm_[lid].status, (unsigned long)pthread_self());
          struct timespec ts;
          clock_gettime(CLOCK_REALTIME, &ts);
          ts.tv_sec += 3;
          int r = pthread_cond_timedwait(&lid_cond, &lid_m, &ts);
          if(r != ETIMEDOUT) {
            VERIFY(r == 0);
          }
          //VERIFY(pthread_cond_wait(&lid_cond, &lid_m) == 0);
        }
      }
      else if(lockm_[lid].status == retry_later) {
        while(!(lockm_[lid].status == free || lockm_[lid].status == none)) {
          struct timespec ts;
          clock_gettime(CLOCK_REALTIME, &ts);
          ts.tv_sec += 3;
          tprintf("acquire %s waiting for lid %llu status %d in retry_later state (%lu)\n", id.c_str(), lid, lockm_[lid].status, (unsigned long)pthread_self());
          int r = pthread_cond_timedwait(&lid_cond, &lid_m, &ts);
          if(r != ETIMEDOUT) {
            VERIFY(r == 0);
          }
          //VERIFY(pthread_cond_wait(&lid_cond, &lid_m) == 0);
          if(lockm_[lid].status == retry_later) {
            //If we have not retried yet, we will retry later after 3 seconds even if we have not received retry RPC from server yet. This is done for lab 7 in event of lock_server failures, server might not send a retry
            tprintf("acquire %s retry waiting time for lid %llu has expired. Now retrying without waiting for retry RPC from lock_server (%lu)\n", id.c_str(), lid, (unsigned long)pthread_self());
            lockm_[lid].status = none;
          }
        }
      }
      if(lockm_[lid].status == none) {
        if(!flagAcquireSentOnce) {
          flagAcquireSentOnce = true;
          lockm_[lid].xid = xid++;
        }
        xid_a = lockm_[lid].xid;
        tprintf("lock_client_cache_rsm: acquire lid %llu assigned sequence %llu\n", lid, xid_a);
        lockm_[lid].status = acquiring;
      }
      else if(lockm_[lid].status == free){
        lockm_[lid].status = locked;
        tprintf("lock_client_cache_rsm: acquire client already has the lock lid %llu and its free. So lock has been acquired: %s (%lu)\n", lid, id.c_str(), (unsigned long)pthread_self());
        acquiredLock = true;
      }
    }

    if(!acquiredLock) {
      tprintf("lock_client_cache_rsm: acquire client acquiring lid %llu:%llu by making a RPC call %s (%lu)\n", lid, xid_a, id.c_str(), (unsigned long)pthread_self());
      int r;
      lock_protocol::status ret = rsmc->call(lock_protocol::acquire, lid, id, xid_a, r);
#ifdef _DEBUG_
      tprintf("lock_client_cache_rsm: acquire client RPC returned for lid %llu:%llu clt:%s ret:%d (errors NOENT %d RPCERROR %d) (%lu)\n", lid, xid_a, id.c_str(), ret, lock_protocol::NOENT, lock_protocol::RPCERR, (unsigned long)pthread_self());
#endif
      VERIFY(ret == lock_protocol::OK || ret == lock_protocol::RETRY);
      {
        ScopedLock lm(&lid_m);
        if(ret == lock_protocol::OK) {
          tprintf("lock_client_cache_rsm: acquired lock %llu via RPC %s (%lu)\n", lid, id.c_str(), (unsigned long)pthread_self());
          lockm_[lid].status = locked;   //Mark it as acquired lock successfully
          acquiredLock = true;
        }
        else if(ret == lock_protocol::RETRY){
          tprintf("lock_client_cache_rsm: acquire lock %llu resulted in retry later %s (%lu)\n", lid, id.c_str(), (unsigned long)pthread_self());
          lockm_[lid].status = retry_later;   //Mark it as acquired lock successfully
          VERIFY(pthread_cond_broadcast(&lid_cond) == 0);  //broadcast it so that revoke thread knows acquire has returned and hence revoke can return
        }
      }
    }
  }
  return lock_protocol::OK;
}

  lock_protocol::status
lock_client_cache_rsm::release(lock_protocol::lockid_t lid)
{
  bool flagRelease = false;
  lock_protocol::xid_t xid_r ;
#ifdef _DEBUG_
  tprintf("lock_client_cache_rsm: release call for lid %llu %s (%lu)\n", lid, id.c_str(), (unsigned long)pthread_self());
#endif
  {
    ScopedLock lm(&lid_m);
    xid_r = lockm_[lid].xid;
    if(lockm_[lid].toRevoke) {
      lockm_[lid].toRevoke = false;
      flagRelease = true;
#ifdef _DEBUG_
      tprintf("lock_client_cache_rsm: release for lid %llu leading to releasing state since revoke has been invoked %s status %d (%lu)\n", lid, id.c_str(), lockm_[lid].status, (unsigned long)pthread_self());
#endif
    }
    else {
#ifdef _DEBUG_
      tprintf("lock_client_cache_rsm: release for lid %llu leading to free state %s (%lu)\n", lid, id.c_str(), (unsigned long)pthread_self());
#endif
      lockm_[lid].status = free;
    }
  }
  if(flagRelease) {
    int r;
    if(lu != 0) {
      lu->dorelease(lid);
    }
#ifdef _DEBUG_
    tprintf("lock_client: release RPC is to be called for lid %llu client %s (%lu)\n", lid, id.c_str(), (unsigned long) pthread_self());
#endif
    //lock_protocol::status ret = cl->call(lock_protocol::release, lid, id, xid_r, r);
    lock_protocol::status ret = rsmc->call(lock_protocol::release, lid, id, xid_r, r);
    tprintf("lock_client: release call return for lid %llu:%llu client %s (%lu)\n", lid, xid_r, id.c_str(), (unsigned long) pthread_self());
    VERIFY (ret == lock_protocol::OK);

    ScopedLock lm(&lid_m);
    lockm_[lid].status = none;  //Release is done, so mark the status as none
  }
  ScopedLock lm(&lid_m);
  //Signal other threads that lid is no longer locked
  VERIFY(pthread_cond_broadcast(&lid_cond) == 0);
  return lock_protocol::OK;
}


  rlock_protocol::status
lock_client_cache_rsm::revoke_handler(lock_protocol::lockid_t lid, 
    lock_protocol::xid_t xid, int &)
{
  int ret = rlock_protocol::OK;
  struct lidlist ls;
  ls.lid = lid;
  ls.xid = xid;
#ifdef _DEBUG_
  tprintf("lock_client_cache_rsm: revoke received for lid %llu %s (%lu)\n", lid, id.c_str(), pthread_self());
#endif
  ScopedLock rm(&revoke_m);
  revoke_id_list.push_back(ls);
  VERIFY(pthread_cond_signal(&revoke_cond) == 0);
  return ret;
}

  rlock_protocol::status
lock_client_cache_rsm::retry_handler(lock_protocol::lockid_t lid, 
    lock_protocol::xid_t xid, int &)
{
  int ret = rlock_protocol::OK;
  struct lidlist ls;
  ls.lid = lid;
  ls.xid = xid;
  tprintf("lock_client_cache_rsm: retry_handler for lid %llu:%llu invoked with status %d %s (%lu)\n", lid, xid, lockm_[lid].status, id.c_str(), pthread_self());
  ScopedLock re(&retry_m);
  retry_id_list.push_back(ls);
  VERIFY(pthread_cond_signal(&retry_cond) == 0);
  return ret;
}
