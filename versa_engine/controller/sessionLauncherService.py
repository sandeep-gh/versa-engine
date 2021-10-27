import sys
import rpyc
import time
import versa_engine.controller.host_controller as hc

server_handle = None


class SessionLauncherService(rpyc.Service):
    def on_connect(self, conn):
        # code that runs when a connection is created
        # (to init the service, if needed)
        print("client connected")
        pass

    def on_disconnect(self, conn):
        print("client disconnected")
        # code that runs after the connection has already closed
        # (to finalize the service, if needed)
        pass

    def exposed_shutdown_proxyService(self):
        server_handle.close()

        pass

    def exposed_launchdbjob(self, **kwargs):  # this is an exposed method
        from versa_engine.jobmanagers import get_jobmanager
        jobmanager = get_jobmanager()
        print ("would have called localhostjobmanager ", kwargs)
        #proxyCon = jobmanager.launchdbjob(kwargs)

        return 5

    def exposed_teardown_session(self, session_id):  # this is an exposed method
        print("proxy called:stopdb ", session_id)
        return hc.stopdb(session_id)


if __name__ == "__main__":
    from rpyc.utils.server import ThreadedServer

    server_handle = ThreadedServer(SessionLauncherService, port=int(sys.argv[1]))
    server_handle.start()
    print("Shutting down proxy server")
