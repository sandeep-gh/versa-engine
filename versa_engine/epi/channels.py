from pyboost_ipc import message_queue, open_or_create
from collections import namedtuple

def create_channel(channel_name=None, maxNumberOfMessages=1024*512, maxMessageSizeBytes=1024*32):
    '''
    Creates a message queue.  If creating a new queue, the number of messages, and the max message size need to be specified.  If the queue already exists, then the number and message size is ignored. 
    Sends a message to the queue to later be retrieved.

    Input:
        queueName: the name of the queue
        maxNumberOfMessages: maximum number of messages that can be in the queue at once.
        maxMessageSizeBytes: maximum size of a message in the queue.
    '''
    vc_t = namedtuple('vq', ['queue', 'name', 'buff'])
    queue = message_queue(open_or_create, channel_name, maxNumberOfMessages, maxMessageSizeBytes)
    buff = bytearray(queue.max_msg_size)
    return vc_t(queue, channel_name, buff)
    
def send_message(vc=None, message=""):
    '''
    Creates a message queue.  If creating a new queue, the number of messages, and the max message size need to be specified.  If the queue already exists, then the number and message size is ignored. 
    Sends a message to the queue to later be retrieved.

    Input:
        queueName: the name of the queue
        message: the message to be sent in the queue
        maxNumberOfMessages: maximum number of messages that can be in the queue at once.
        maxMessageSizeBytes: maximum size of a message in the queue.

    '''
    # if maxMessageSizeBytes==None or  maxMessageSizeBytes < len(message):
    #         maxMessageSizeBytes= 2*len(message)
    #     queue = message_queue(open_or_create, queueName, maxNumberOfMessages, maxMessageSizeBytes)
    if len(message)> vc.queue.max_msg_size:
        print ("Your message is of size"+str(len(message)) +"and you tried to pass it into queue "+vc.name+" with a size of "+str(maxMessageSizeBytes))
    vc.queue.send(message, 1)



def receive_message(vc=None):
    '''
    It then waits for a message to added to the queue.
    
    Inputs:
        queueName: the name of the queue
    '''
    #queue = message_queue(open_or_create, queueName, maxNumberOfMessages, maxMessageSizeBytes)
    #buff = bytearray(vq.queue.max_msg_size)
    rcvd_size, priority = vc.queue.receive(vc.buff)
    return vc.buff[0:rcvd_size]

