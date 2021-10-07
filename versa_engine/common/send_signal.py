import utilities
import sys
import socket
server = sys.argv[1]
port = int(sys.argv[2])
msg='msg from' + socket.gethostname()
if len(sys.argv) == 4:
    msg = sys.argv[3]
utilities.signal_send(server, port, msg)
