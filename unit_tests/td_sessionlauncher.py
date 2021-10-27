import rpyc
conn = rpyc.connect("localhost",  9878)

res =  conn.root.launchdbjob(a=1, b=2,c=3)

conn.close()
