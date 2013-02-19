import Queue
import collections

import numpy as np


BYTE_ORDER_MARK = u'\uFEFF'
BYTE_ORDER_TEXT = '%8g' % ord(BYTE_ORDER_MARK)

format_8g = np.frompyfunc(lambda x: '%8g' % x, 1, 1)


class DataBuffer(object):
#========================

  def __init__(self):
  #------------------
    self._queue = Queue.Queue()
    self._data = [ ]
    self._datalen = 0
    self._pos = 0

  def put(self, data):
  #-------------------
    self._queue.put(data)

  def next(self):
  #--------------
    while self._pos >= self._datalen:
      data = self._queue.get()
      if data is None: raise StopIteration
      self._data = format_8g(data)
      self._datalen = len(data)
      self._pos = 0
    self._pos += 1
    return self._data[self._pos - 1]


class TextBuffer(object):
#========================

  def __init__(self):
  #------------------
    self._queue = collections.deque()  # We don't need a Queue() since we never
    self._data = [ ]                   # wait if no text to send.
    self._pos = 0

  def put(self, text):
  #-------------------
    self._queue.append(text)

  def next(self):
  #--------------
    while len(self._data) == 0:
      if len(self._queue) == 0:
        return BYTE_ORDER_TEXT
      text = self._queue.popleft()
      if text is None: raise StopIteration
      self._data = [ '%8g' % ord(c) for c in text ]
      self._pos = 0
    self._pos += 1
    return self._data[self._pos - 1]


class FrameStream(object):
#=========================

  def __init__(self, channels, no_text=False):
  #-------------------------------------------
    self._databuf = [ DataBuffer() for n in xrange(channels) ]
    self._textbuf = None if no_text else TextBuffer()

  def put_data(self, channel, data):
  #---------------------------------
    self._databuf[channel].put(data)

  def put_text(self, text):
  #------------------------
    if self._textbuf is not None:
      self._textbuf.put(text)

  def frames(self):
  #----------------
    framecount = 0
    try:
      while True:
        if self._textbuf is not None:
          line = [ str(framecount) ] + [ b.next() for b in self._databuf ] + [ self._textbuf.next() ]
        else:
          line = [ str(framecount) ] + [ b.next() for b in self._databuf ]
        yield ' '.join(line)
        framecount += 1
    except StopIteration:
      pass


if __name__ == '__main__':
#=========================

  op = FrameStream(3)
  op.put_data(0, [  1,  2,  3,  4,  5, 6, 7 ])
  op.put_data(1, [ 11, 12, 13, 14, 15 ])
  op.put_data(2, [ 21, 22, 23 ])
  op.put_data(0, [  1,  2,  3,  4,  5, 6, 7 ])
  op.put_data(1, [  1,  2,  3,  4,  5, 6, 7 ])
  op.put_data(2, [  1,  2,  3,  4,  5, 6, 7 ])
  op.put_text('123A')
  op.put_text('123A')
  op.put_data(2, [ 4, 5 ])

  op.put_data(0, None)
  op.put_data(1, None)
  op.put_data(2, None)

  for f in op.frames():
    print f
