import random
import socket
import thread
from threading import Thread
import sys
import time
import sha
import hashlib
import traceback
import copy
from threading import Lock

k = 160
MAX = 2**k

DEFAULT_IP = "192.168.1.5"
DEFAULT_PORT = 4567

HASH_LENGTH = 160
finger = {}
fingerLock = Lock()

#if enter printnode in the command, all the nodes of the ring will print

def printNodes(node):
    print ' Ring nodes:'
    end = node
    print end.key
    while end.key != node.successor().key:
        end.key = end.successor().key
        print end.key

    print "******************************"

def get_key(ip, port):
    line = str(ip) + str(port)
    key=long(sha.new(line).hexdigest(),16)
    return key

class Node:
    def __init__(self, ip, port):
        global fingerLock
        self.ip = ip
        self.port = port
        self.key = get_key(ip, port)
        self.finger = {}
        self.start = {}

        fingerLock.acquire()
        self.predecessor = self
        for i in range(HASH_LENGTH):
            self.start[i] = (self.key+(2**i)) % (2**HASH_LENGTH)
        fingerLock.release()

    def successor(self):
        return self.finger[0]

    def find_successor(self,key):  

        n1 = self
        if key == self.key:
            return n1
        elif self.predecessor.key > self.key:
            shift = MAX - init
            self.key = (self.key + shift) % MAX
            key = (key + shift) % MAX
        
        if key < self.key:
            return n1
        else:
            n1 = self.find_predecessor(key)
            return n1.successor()

    def find_predecessor(self,key):

        if key == self.key:
            return self.predecessor
        n1 = self

        while True:
            if key == n1.successor().key or n1.successor().key == n1.key:
                n1 = self
            elif n1.key > n1.successor().key:
                shift = MAX - n1.key
                n1.successor().key = (n1.successor().key + shift) % MAX
                key = (key + shift) % MAX

            if key < n1.successor().key:
                n1 = self
            else:
                n1 = n1.closest_preceding_finger(key)

        return n1

    
    def closest_preceding_finger(self,key):
        global fingerLock
        
        for i in range(HASH_LENGTH-1,-1,-1):
            if self.key == key:
                return self.finger[0]
            elif self.key > key:
                shift = MAX - self.key
                init = 0
                key = (key + shift) % MAX

                fingerLock.acquire()
                self.finger[i].key = (self.finger[i].key + shift) % MAX
                fingerLock.release()

                if self.key < self.finger[i].key and self.finger[i].key < key:
                    return self.finger[0]
        return self
           
    def join(self,n1):

        global fingerLock

        if self.key == n1.key:

            fingerLock.acquire()
            for i in range(HASH_LENGTH):
                self.finger[i] = self
            self.predecessor = self
            fingerLock.release()

        else:
            send_message(n1.ip, n1.port)

            fingerLock.acquire()
            self.finger[0] = n1.find_successor(self.start[0])
            self.predecessor = self.successor().predecessor
            self.successor().predecessor = self
            self.predecessor.finger[0] = self

            for i in range(HASH_LENGTH-1):
                
                if between(self.start[i+1],self.key,self.finger[i].key):
                    self.finger[i+1] = self.finger[i]
                else :
                    self.finger[i+1] = n1.find_successor(self.start[i+1])
                
            fingerLock.release()

            for i in range(HASH_LENGTH):
                if self.key >= 2**i:
                    prev = self.key - 2**i
                else:
                    prev = MAX - (2**i - self.key)
                p = self.find_predecessor(prev)

                if prev == p.successor().key:
                    p = p.successor()
                p.fix_fingers(self,i)

    def fix_fingers(self,s,i):
        
        global fingerLock

        if s.key == self.finger[i].key or self.finger[i].key == self.key:
           if self.key != s.key:
              fingerLock.acquire()
              self.finger[i] = s
              fingerLock.release()

              p = self.predecessor
              p.fix_fingers(s,i)
        elif self.key > self.finger[i].key:
            shift = MAX - self.key
            self.finger[i].key = (self.finger[i].key + shift) % MAX
            s.key = (s.key + shift) % MAX
        
        if self.key < self.finger[i].key:
           if self.key != s.key:
              fingerLock.acquire()
              self.finger[i] = s
              fingerLock.release()

              p = self.predecessor
              p.fix_fingers(s,i)

    def update_others_leave(self):
        for i in range(HASH_LENGTH):
            if self.key >= 2**i:
                prev = self.key - 2**i
            else:
                prev = MAX - (2**i - self.key)
            p = self.find_predecessor(prev)
            p.fix_fingers(self.successor(),i)

    def leave(self):
        self.successor().predecessor = self.predecessor
        self.predecessor.setSuccessor(self.successor())
        self.update_others_leave()

    def setSuccessor(self,succ):
        global fingerLock

        fingerLock.acquire()
        self.finger[0] = succ
        fingerLock.release()

def print_finger_table(node):
    print 'Finger Table of ' + str(node.key)
    for i in range(HASH_LENGTH):
        print str(node.start[i]) +' : ' +str(node.finger[i].key)  
    print "***************************************************"

def between(value,init,end):
    if value == init or init == end:
        return True
    elif init > end :
         shift = MAX - init
         end = (end +shift)%MAX
         value = (value + shift)%MAX
    if value < end:
        return True
    else:
        return False

def send_message(ip, port):
    UDP_IP = ip
    UDP_PORT = port
    MESSAGE = "Hello, World!"
    print "message:" + MESSAGE
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.sendto(MESSAGE, (UDP_IP, UDP_PORT))

#A new thread needed for this method, it's used for receive message

def reveive_message(ip, port):
    UDP_IP = ip
    UDP_PORT = port 
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.bind((UDP_IP, UDP_PORT))
    except:
        traceback.print_exc()
        print "no connect"
        return None
    while True:
        data, addr = sock.recvfrom(1024)
        print "received message:", data
    return data

def set_chord(node):
    recv = send_message(node.ip, node.port)  

if __name__ == "__main__":

    knowNode = Node(DEFAULT_IP, DEFAULT_PORT)
    port = 0
    MY_IP = ""
    
    if (len(sys.argv) == 2):
        port = int(sys.argv[-1])
        MY_IP = socket.gethostbyname(socket.gethostname())
    else:
        port = DEFAULT_PORT
        MY_IP = DEFAULT_IP

    myNode = Node(MY_IP, port)
    
    fingerLock.acquire()
    for i in range(0, HASH_LENGTH):
        finger[i] = copy.deepcopy(myNode.key)
    fingerLock.release()

    answerThread = Thread(target=reveive_message, args=(MY_IP, port))
    answerThread.daemon = True
    answerThread.start()
    knowNode.join(knowNode)

    myNode.join(knowNode)

    while True:
        command = raw_input("Command>> ")
        command = command.lower()
        if len(command) == 0:
            pass
        elif command in ("fingertable"):
            print_finger_table(myNode)
        elif command in ("printnode"):
            printNodes(myNode)
        elif command in ("leave"):
            myNode.leave()
        elif command in ("set"):
            set_chord(myNode)