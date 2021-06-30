
import socket
import sys
def start_port_server():
    s = socket.socket()
    host ='127.0.0.1'
    port =  39785
    s.bind((host, port))
    s.listen(5)
    port_ctr=5561
    while (1):
        c, addr = s.accept()     # Establish connection with client.
        print ('Got connection from', addr)
        c.send(str(port_ctr).encode())
        port_ctr=port_ctr+1

        c.close()                # Close the connection



if __name__ == '__main__':
    start_port_server()
