import os
import versa_communication as vc

def communicationTest():
    newpid = os.fork()
    if newpid == 0:
        sender()
    else:
        reciever()



def sender():
    vc.send_message(queueName="messageQueue", message="The message being sent",maxNumberOfMessages=1, maxMessageSizeBytes=30)


def reciever():
    message=vc.receive_message(queueName="messageQueue", maxNumberOfMessages=1, maxMessageSizeBytes=30)
    assert(message =="The message being sent")
    print message


communicationTest()
