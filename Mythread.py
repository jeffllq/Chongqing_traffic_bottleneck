import threading

class MyThread(threading.Thread):

    def __init__(self, threadID, road_queue=None, target=None):
        threading.Thread.__init__(self, target=target)
        self.threadID = threadID
        self.road_queue = road_queue

    def run(self):
        threadLock.acquire()



if __name__ == '__main__':
    threadLock = threading.Lock()
    threading.Thread()