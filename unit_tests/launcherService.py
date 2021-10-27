import rpyc

conn = rpyc.connect("192.168.1.159", 8989)
res = conn.root.launchdbjob(a=1, b=2, c=3)

print(res)
