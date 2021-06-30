import sys
import rpyc
import time
import controller.host_controller as hc


class VersaProxyService(rpyc.Service):
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

    def exposed_pingdb(self, session_id):  # this is an exposed method
        print("proxy called: pingdb")
        return hc.pingdb(session_id)

    def exposed_stopdb(self, session_id):  # this is an exposed method
        print("proxy called:stopdb")
        return hc.stopdb(session_id)

    def exposed_startdb(self, session_id):
        print("proxy called:startdb")
        return hc.startdb(session_id)


if __name__ == "__main__":
    from rpyc.utils.server import ThreadedServer
    t = ThreadedServer(VersaProxyService, port=int(sys.argv[1]))
    t.start()
    print("Shutting down proxy server")
