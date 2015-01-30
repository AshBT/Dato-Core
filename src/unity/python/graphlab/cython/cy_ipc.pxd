from libcpp.vector cimport vector
from libcpp.string cimport string 


cdef extern from "<cppipc/common/message_types.hpp>" namespace 'cppipc':
    cdef cppclass reply_status:
        pass

cdef extern from "<cppipc/client/comm_client.hpp>" namespace 'cppipc':
    cdef cppclass comm_client:
        comm_client(vector[string] zkhosts, string name, unsigned int max_init_ping_failures,
                      string alternate_control_address, string alternate_publish_address, 
                      string public_key, string secret_key, string server_public_key,
                      ops_interruptible) except +
        void add_auth_method_token(string authtoken) except +
        void add_status_watch(string prefix, void(*callback)(string)) except +
        void remove_status_watch(string prefix) except +
        reply_status start() except + 
        void stop() except +

cdef extern from "cppipc/common/message_types.hpp" namespace 'cppipc':
    cdef string reply_status_to_string(reply_status)

cdef extern from "<zmq_utils.h>":
    cdef string zmq_curve_keypair(char*, char*)

cdef void print_status(string status_string) nogil

cdef class PyCommClient:
    cdef comm_client *thisptr 
    cpdef add_auth_method_token(self, string authtoken)
    cpdef set_log_progress(self, bint is_on)
    cpdef stop(self)
    cpdef start(self)
