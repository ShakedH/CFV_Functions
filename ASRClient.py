import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'env/Lib/site-packages')))

import json  # json
import threading  # multi threading
import Queue  # queue used for thread syncronization
import argparse  # for parsing arguments
import base64  # necessary to encode in base64
import requests  # python HTTP requests library
from autobahn.twisted.websocket import WebSocketClientProtocol, \
    WebSocketClientFactory, connectWS
from twisted.internet import ssl, reactor

from urllib2 import urlopen
import itertools
from azure.storage.queue import QueueService


class Utils:

    @staticmethod
    def getAuthenticationToken(hostname, serviceName, username, password):
        fmt = hostname + "{0}/authorization/api/v1/token?url={0}/{1}/api"
        uri = fmt.format(hostname, serviceName)
        uri = uri.replace("wss://", "https://").replace("ws://", "https://")
        print(uri)
        auth = (username, password)
        headers = {'Accept': 'application/json'}
        resp = requests.get(uri, auth=auth, verify=False, headers=headers,
                            timeout=(30, 30))
        print(resp.text)
        jsonObject = resp.json()
        return jsonObject['token']


class WSInterfaceFactory(WebSocketClientFactory):

    def __init__(self, queue, summary, dirOutput, contentType, model,
                 url=None, headers=None, debug=None):

        WebSocketClientFactory.__init__(self, url=url, headers=headers)
        self.queue = queue
        self.summary = summary
        self.dirOutput = dirOutput
        self.contentType = contentType
        self.model = model
        self.queueProto = Queue.Queue()

        self.openHandshakeTimeout = 10
        self.closeHandshakeTimeout = 10

        # start the thread that takes care of ending the reactor so
        # the script can finish automatically (without ctrl+c)
        endingThread = threading.Thread(target=self.endReactor, args=())
        endingThread.daemon = True
        endingThread.start()

    def prepareUtterance(self):

        try:
            utt = self.queue.get_nowait()
            self.queueProto.put(utt)
            return True
        except Queue.Empty:
            print("getUtterance: no more utterances to process, queue is "
                  "empty!")
            return False

    def endReactor(self):

        self.queue.join()
        print("about to stop the reactor!")
        reactor.stop()

    # this function gets called every time connectWS is called (once
    # per WebSocket connection/session)
    def buildProtocol(self, addr):

        try:
            utt = self.queueProto.get_nowait()
            proto = WSInterfaceProtocol(self, self.queue, self.summary,
                                        self.dirOutput, self.contentType)
            proto.setUtterance(utt)
            return proto
        except Queue.Empty:
            print("queue should not be empty, otherwise this function should "
                  "not have been called")
            return None


# WebSockets interface to the STT service
#
# note: an object of this class is created for each WebSocket
# connection, every time we call connectWS
class WSInterfaceProtocol(WebSocketClientProtocol):

    def __init__(self, factory, queue, summary, dirOutput, contentType):
        self.factory = factory
        self.queue = queue
        self.summary = summary
        self.dirOutput = dirOutput
        self.contentType = contentType
        self.packetRate = 20
        self.listeningMessages = 0
        self.timeFirstInterim = -1
        self.bytesSent = 0
        self.chunkSize = 2000  # in bytes
        super(self.__class__, self).__init__()
        print(dirOutput)
        print("contentType: {} queueSize: {}".format(self.contentType,
                                                     self.queue.qsize()))

    def setUtterance(self, utt):

        self.uttNumber = utt[0]
        self.uttFilename = utt[1]
        self.summary[self.uttNumber] = {"hypothesis": "",
                                        "status": {"code": "", "reason": ""}}
        self.fileJson = "{}/{}.json".format(self.dirOutput, self.uttNumber)
        try:
            os.remove(self.fileJson)
        except OSError:
            pass

    # helper method that sends a chunk of audio if needed (as required
    # what the specified pacing is)
    def maybeSendChunk(self, data):

        def sendChunk(chunk, final=False):
            self.bytesSent += len(chunk)
            self.sendMessage(chunk, isBinary=True)
            if final:
                self.sendMessage(b'', isBinary=True)

        if (self.bytesSent + self.chunkSize >= len(data)):
            if (len(data) > self.bytesSent):
                sendChunk(data[self.bytesSent:len(data)], True)
                return
        sendChunk(data[self.bytesSent:self.bytesSent + self.chunkSize])
        self.factory.reactor.callLater(0.01, self.maybeSendChunk, data=data)
        return

    def onConnect(self, response):
        print("onConnect, server connected: {}".format(response.peer))

    def onOpen(self):
        print("onOpen")
        data = {"action": "start",
                "content-type": str(self.contentType),
                "continuous": True,
                "interim_results": False,
                "inactivity_timeout": 2500,
                'max_alternatives': 0,
                'timestamps': True,
                'word_confidence': False}
        print("sendMessage(init)")
        # send the initialization parameters
        self.sendMessage(json.dumps(data).encode('utf8'))

        # start sending audio right away (it will get buffered in the
        # STT service)
        print(self.uttFilename)
        audio_file_url = r"https://{0}.blob.core.windows.net/{1}/{2}".format(account_name, audio_container_name,
                                                                             self.uttFilename)
        audio_file_object = urlopen(audio_file_url)
        dataFile = audio_file_object.read()
        self.maybeSendChunk(dataFile)
        print("onOpen ends")

    def onMessage(self, payload, isBinary):

        if isBinary:
            print("Binary message received: {0} bytes".format(len(payload)))
        else:
            print(u"Text message received: {0}".format(payload.decode('utf8')))

            # if uninitialized, receive the initialization response
            # from the server
            jsonObject = json.loads(payload.decode('utf8'))
            if 'state' in jsonObject:
                self.listeningMessages += 1
                if self.listeningMessages == 2:
                    print("sending close 1000")
                    # close the connection
                    self.sendClose(1000)

            # if in streaming
            elif 'results' in jsonObject:
                jsonObject = json.loads(payload.decode('utf8'))
                hypothesis = ""
                # empty hypothesis
                if len(jsonObject['results']) == 0:
                    print("empty hypothesis!")
                # regular hypothesis
                else:
                    # dump the message to the output directory
                    jsonObject = json.loads(payload.decode('utf8'))
                    # with open(self.fileJson, "a") as f:
                    #     f.write(json.dumps(jsonObject, indent=4,
                    #                        sort_keys=True))
                    res = jsonObject['results'][0]
                    hypothesis = '. '.join(
                        [result['alternatives'][0]['transcript'].strip() for result in jsonObject['results']])
                    timestamps = list(itertools.chain.from_iterable(
                        [results['alternatives'][0]['timestamps'] for results in jsonObject['results']]))
                    bFinal = (res['final'] is True)
                    if bFinal:
                        print('final hypothesis: "' + hypothesis + '"')
                        self.summary[self.uttNumber]['hypothesis'] += hypothesis
                        self.summary['timestamps'] = timestamps
                    else:
                        print('interim hyp: "' + hypothesis + '"')

    def onClose(self, wasClean, code, reason):

        print("onClose")
        print("WebSocket connection closed: {0}, code: {1}, clean: {2}, "
              "reason: {0}".format(reason, code, wasClean))
        self.summary[self.uttNumber]['status']['code'] = code
        self.summary[self.uttNumber]['status']['reason'] = reason

        # create a new WebSocket connection if there are still
        # utterances in the queue that need to be processed
        self.queue.task_done()

        if not self.factory.prepareUtterance():
            return

        # SSL client context: default
        if self.factory.isSecure:
            contextFactory = ssl.ClientContextFactory()
        else:
            contextFactory = None
        connectWS(self.factory, contextFactory)


# function to check that a value is a positive integer
def check_positive_int(value):
    ivalue = int(value)
    if ivalue < 1:
        raise argparse.ArgumentTypeError(
            '"%s" is an invalid positive int value' % value)
    return ivalue


# function to check the credentials format
def check_credentials(credentials):
    elements = credentials.split(":")
    if len(elements) == 2:
        return elements
    else:
        raise argparse.ArgumentTypeError(
            '"%s" is not a valid format for the credentials ' % credentials)


account_name = 'cfvtes9c07'
audio_container_name = "audio-segments-container"
asr_credentials = '853a3e00-bd09-4d31-8b78-312058948303:YOBwYe01gUeG'


def get_transcript(file_name):
    content = 'audio/wav'
    model = 'en-US_BroadbandModel'
    threads = 1
    # Not relevant but necessary:
    diroutput = 'output'

    q = Queue.Queue()
    q.put((0, file_name))

    hostname = "stream.watsonplatform.net"
    headers = {}
    # authentication header
    auth = asr_credentials
    headers["Authorization"] = "Basic " + base64.b64encode(auth)
    # create a WS server factory with our protocol
    fmt = "wss://{}/speech-to-text/api/v1/recognize?model={}"
    url = fmt.format(hostname, model)
    summary = {}
    factory = WSInterfaceFactory(q, summary, diroutput, content,
                                 model, url, headers, debug=False)
    factory.protocol = WSInterfaceProtocol

    for i in range(min(int(threads), q.qsize())):

        factory.prepareUtterance()

        # SSL client context: default
        if factory.isSecure:
            contextFactory = ssl.ClientContextFactory()
        else:
            contextFactory = None
        connectWS(factory, contextFactory)

    reactor.run()

    emptyHypotheses = 0
    return {
        'file_name':file_name,
        'transcript': summary[0]['hypothesis'],
        'timestamps': summary['timestamps']
    }


def update_start_time(data, start_time):
    new_data = data.copy()
    new_timestamps = [[record[0], record[1] + start_time, record[2] + start_time] for record in data['timestamps']]
    new_data['timestamps'] = new_timestamps
    return new_data


def enqueue_message(qname, message):
    storage_acc_name = 'cfvtes9c07'
    storage_acc_key = 'DSTJn6a1dS9aaoJuuw6ZOsnrsiW9V1jODJyHtekkYkc3BWofGVQjS6/ICWO7v51VUpTHSoiZXVvDI66uqTnOJQ=='
    message = base64.b64encode(message.encode('ascii')).decode()
    queue_service = QueueService(account_name=storage_acc_name, account_key=storage_acc_key)
    queue_service.put_message(qname, message)


if __name__ == '__main__':
    inputMessage = open(os.environ['inputMessage']).read()
    message_obj = json.loads(inputMessage)
    print('started function\nFile Name: ' + message_obj['file_name'] + '\nStart_time: ' + str(
        message_obj['start_time']))
    data = get_transcript(file_name=message_obj['file_name'])
    data = update_start_time(data, message_obj['start_time'])
    print(data)
    enqueue_message('indexq', json.dumps(data))
