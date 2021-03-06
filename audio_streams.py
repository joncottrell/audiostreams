#!/usr/bin/env python
#coding:utf-8
 
import sys, os, re, time
import base64, binascii
import logging
import Queue
import socket

from tornado.ioloop import IOLoop
from tornado.ioloop import PeriodicCallback
from tornado.iostream import IOStream
from tornado.netutil import TCPServer
  
#logging.basicConfig(filename='audio_streams.log',level=logging.INFO, format='%(levelname)s - - %(asctime)s %(message)s', datefmt='[%d/%b/%Y %H:%M:%S]')
logging.basicConfig(level=logging.INFO, format='%(levelname)s - - %(asctime)s %(message)s', datefmt='[%d/%b/%Y %H:%M:%S]')

class AudioStreamServer(TCPServer):
 
  def __init__(self, io_loop=None, ssl_options=None, **kwargs):
    logging.info('audio_streams server started')
    TCPServer.__init__(self, io_loop=io_loop, ssl_options=ssl_options, **kwargs)
 
  def handle_stream(self, stream, address):
    AudioStreamConnection(stream, address)
 
class AudioStreamConnection(object):
  BYTES_PER_READ = 2304
  KBPS = 128
  
  stream_set = set([])
  connection_set = set([])
 
  def __init__(self, stream, address):
    logging.info('connection from %s', address)
    self.stream = stream
    self.address = address
    self.stream_show_id = None
    self.stream_set.add(self.stream)
    self.connection_set.add(self)
    self.stream.set_close_callback(self._on_close)
    self.stream.read_until('\n', self._on_read_show_id)
    self.icecastClient = None
    
  def _on_read_show_id(self, data):
    isBroadcasting = False
    show_id = data.strip()
    for connection in self.connection_set:
      if show_id == connection.stream_show_id:
        isBroadcasting = True
        break
    #TODO: later - verify that is an actual show object id because mountpoints will be named after object id
    if not isBroadcasting:
      self.stream_show_id = show_id
      self.icecastClient = IcecastSourceClient.getIcecastSourceClient(self.stream_show_id, self.KBPS, self)
      logging.info('show_id:%s', self.stream_show_id)
      self.stream.write('OK\r\n', self._on_stream_ready)
    else:
      logging.info('show_id already taken: %s', show_id)
      self.stream.write('Error: "'+ show_id +'" currently streaming\n', self.stream.close)
 
  def _on_stream_ready(self):
    self.stream.read_bytes(self.BYTES_PER_READ, self._on_read_complete)
 
  def _on_read_complete(self, data):
    binary_audio = data
    self.send_icecast(binary_audio)
    self.stream.read_bytes(self.BYTES_PER_READ, self._on_read_complete)
 
  def send_icecast(self, data):
    self.icecastClient.add_audio(data)
    
  def _on_write_complete(self):
    logging.info('write line to %s', self.address)
    if not self.stream.reading():
      self.stream.read_until('\n', self._on_read_line)
 
  def _on_close(self):
    logging.info('%s-connection closed (address: %s)', self.stream_show_id, self.address)
    self.stream_set.remove(self.stream)
    self.connection_set.remove(self)
    self.icecastClient.isFinishing = True
    
class IcecastSourceClient(object):
  BUFFER_TIME = 3.0
  
  icecast_source_client_set = set([])
  
  def __init__(self, stream_id, kbps, audiostream_connection):
    self.stream_id = stream_id
    self.connection = audiostream_connection
    self.didStart = False
    self.isFinishing = False
    self.kbps = kbps
    self.queue = Queue.Queue()
    IcecastSourceClient.icecast_source_client_set.add(self)
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
    self.stream = IOStream(s)
    self.stream.set_close_callback(self._on_close)
    self.stream.connect(("localhost", 8000), self.connect)
    self.curr_queue_time = 0.0
    self.periodic = PeriodicCallback(self.manage_audio, self.bytes2time(1000*AudioStreamConnection.BYTES_PER_READ), IOLoop.instance())
  
  @staticmethod
  def getIcecastSourceClient(stream_id, kbps, audiostream_connection):
    for source_client in IcecastSourceClient.icecast_source_client_set:
      if source_client.stream_id == stream_id:
        source_client.isFinishing = False
        return source_client
        
    return IcecastSourceClient(stream_id, kbps, audiostream_connection)
    
  def connect(self):
    logging.info('%s-icecast port connected', self.stream_id)
    self.stream.write(("SOURCE /%s HTTP/1.0\n"
    "Authorization: Basic c291cmNlOnRlc3RpbmcjIyNzcGFjZWJhcg==\n"
    "User-Agent: libshout/2.3.1\n"
    "Content-Type: audio/mpeg\n"
    "ice-description: HLS Test\n\n") % self.stream_id)
    self.stream.read_until("\n", self.on_response)
    self.periodic.start()
    
  def add_audio(self, data):
    self.queue.put(data)
    self.curr_queue_time += self.bytes2time(len(data))
    logging.info('%s-received,BUFFER:%f', self.stream_id, self.curr_queue_time)
    
  def manage_audio(self):
    if self.stream.closed():
      self.connection.stream.close()
      self.periodic.stop()
      return
    
    if not self.didStart:
      if self.curr_queue_time > IcecastSourceClient.BUFFER_TIME:
        self.didStart = True
      else:
        return
    
    if not self.isFinishing and self.curr_queue_time < 1.0:
      logging.info('%s - Halt sending to refill buffer', self.stream_id)
      return
    
    if self.queue.empty() and self.isFinishing:
      logging.info('%s - sent all data' % self.stream_id)
      self.periodic.stop()
      self.stream.close()
    else:
      data = self.queue.get()
      self.curr_queue_time -= self.bytes2time(len(data))
      logging.info('%s-sending, BUFFER:%f', self.stream_id, self.curr_queue_time)
      self.stream.write(data)
        
  def bytes2time(self, num_bytes):
    return 8*num_bytes/(self.kbps*1024.0)
    
  def on_response(self, data):
    logging.info('%s-Received from icecast: %s', self.stream_id, data)
    
  def _on_close(self):
    logging.info('%s-closed icecast stream', self.stream_id)
    IcecastSourceClient.icecast_source_client_set.remove(self)
 
def main():
  audio_stream_server = AudioStreamServer()
  audio_stream_server.listen(8888)
  IOLoop.instance().start()
 
if __name__ == '__main__':
  main()
