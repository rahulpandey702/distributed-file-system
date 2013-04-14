#ifndef lock_server_cache_rsm_h
#define lock_server_cache_rsm_h

#include <string>

#include "lock_protocol.h"
#include "lock_protocol.h"
#include "rpc.h"
#include "rsm_state_transfer.h"
#include "rsm.h"

class lock_server_cache_rsm : public rsm_state_transfer {
 private:
   pthread_mutex_t lid_m, revoke_m, retry_m;
   pthread_cond_t lid_cond, revoke_cond, retry_cond;
   struct waitingClt {
     std::string cltID;
     lock_protocol::xid_t xid;
   };
   struct lockstat {
     std::string cltID;    //The ID string of the client currently holding the lock
     std::list<struct waitingClt>waitingCltIDs;   //The list of IDs waiting for lock to be released
     bool revoke;
     int stats;
     lock_protocol::xid_t xid;
   };
   std::map<lock_protocol::lockid_t, struct lockstat> lockstat_;
   void freelocks();

   struct revoke {
     std::string cltID;
     lock_protocol::lockid_t lid;
   };
   std::list<struct revoke>revoke_list;
   std::list<lock_protocol::lockid_t> release_lid_list; //List of locks that have been revoked
   rlock_protocol::status makeclientRPC(int, lock_protocol::lockid_t, std::string, lock_protocol::xid_t );

   int nacquire;
   class rsm *rsm;

 public:
   lock_server_cache_rsm(class rsm *rsm);
   lock_protocol::status stat(lock_protocol::lockid_t, int &);
   void revoker();
   void retryer();
   std::string marshal_state();
   void unmarshal_state(std::string state);
   int acquire(lock_protocol::lockid_t, std::string id, 
       lock_protocol::xid_t, int &);
   int release(lock_protocol::lockid_t, std::string id, lock_protocol::xid_t,
       int &);
};

#endif
