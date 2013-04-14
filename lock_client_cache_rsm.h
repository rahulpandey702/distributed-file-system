// lock client interface.

#ifndef lock_client_cache_rsm_h

#define lock_client_cache_rsm_h

#include <string>
#include "lock_protocol.h"
#include "extent_client_cache.h"
#include "rpc.h"
#include "lock_client.h"
#include "lang/verify.h"

#include "rsm_client.h"

// Classes that inherit lock_release_user can override dorelease so that 
// that they will be called when lock_client releases a lock.
// You will not need to do anything with this class until Lab 5.
class lock_release_user_rsm {
 public:
  virtual void dorelease(lock_protocol::lockid_t) = 0;
  virtual ~lock_release_user_rsm() {};
};

class lock_release_flush_rsm : public lock_release_user_rsm {
  private:
    extent_client_cache *ec;
  public:
    lock_release_flush_rsm(extent_client_cache *);
    void dorelease(lock_protocol::lockid_t);
};

class lock_client_cache_rsm;

// Clients that caches locks.  The server can revoke locks using 
// lock_revoke_server.
class lock_client_cache_rsm : public lock_client {
  private:
    rsm_client *rsmc;
    class lock_release_user_rsm *lu;
    int rlock_port;
    std::string hostname;
    std::string id;
    lock_protocol::xid_t xid;
    enum lock_status {
      none=0,
      free,
      locked,
      acquiring,
      releasing,
      retry_later
    };
    struct lockstatus {
      int status;
      bool toRevoke;
      lock_protocol::xid_t xid;
    };
    struct lidlist {
      lock_protocol::xid_t xid;
      lock_protocol::lockid_t lid;
    };
    std::map<lock_protocol::lockid_t,struct lockstatus> lockm_;
    std::list<struct lidlist>revoke_id_list;
    std::list<struct lidlist>retry_id_list;
    pthread_mutex_t lid_m, revoke_m, retry_m;
    pthread_cond_t revoke_cond, retry_cond, lid_cond;
    pthread_t tidRevoke, tidRetry;
  public:
    static int last_port;
    lock_client_cache_rsm(std::string xdst, class lock_release_user_rsm *l = 0);
    virtual ~lock_client_cache_rsm() {};
    lock_protocol::status acquire(lock_protocol::lockid_t);
    virtual lock_protocol::status release(lock_protocol::lockid_t);
    void releaser();
    void revoker();
    rlock_protocol::status revoke_handler(lock_protocol::lockid_t, 
        lock_protocol::xid_t, int &);
    rlock_protocol::status retry_handler(lock_protocol::lockid_t, 
        lock_protocol::xid_t, int &);
};


#endif
