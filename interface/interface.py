import os, sys
import errno
import select
import logging
import urlparse
import multiprocessing
from multiprocessing import Process, Event, Lock
import signal as sighandler

from biosignalml.client import Repository
from biosignalml.units import get_units_uri

import language
import framestream

VERSION = '0.5.0'

BUFFER_SIZE = 10000

##Stream signals at the given RATE.
##Otherwise all URIs must be for signals from the one BioSignalML recording.


_interrupted = Event()

def interrupt(signum, frame):
#============================
  _interrupted.set()


class SignalReader(Process):
#===========================

  def __init__(self, signal, output, channel, ratechecker, **options):
  #-------------------------------------------------------------------
    super(SignalReader, self).__init__()
    self._signal = signal
    self._output = output
    self._channel = channel
    self._options = options
    self._ratechecker = ratechecker

  def run(self):
  #-------------
    logging.debug("Starting channel %d", self._channel)
    try:
      for ts in self._signal.read(**self._options):
        if _interrupted.is_set(): break
        if ts.is_uniform:
          self._ratechecker.check(ts.rate)
          self._output.put_data(self._channel, ts.data)
        else:
          self._ratechecker.check(None)
          self._output.put_data(self._channel, ts.points)
    except Exception, err:
      logging.debug("ERROR: %s", err)
    finally:
      logging.debug("Reader exit... %d", self._channel)
      self._output.put_data(self._channel, None)
      self._signal.close()
      logging.debug("Finished channel %d", self._channel)


class RateChecker(object):
#=========================

  def __init__(self, rate=None):
  #----------------------------
    self._lock = Lock()
    self._rate = rate

  def check(self, rate):
  #---------------------
    if self._rate is None:
      self._lock.acquire()
      self._rate = rate
      self._lock.release()
    elif self._rate != rate:
      logging.debug("%s != %s", self._rate, rate)
      _interrupted.set()
      raise ValueError("Signal rates don't match")


class OutputStream(Process):
#===========================

  def __init__(self, recording, signals, units, rate, dtypes, segment, nometadata, pipe, binary=False):
  #----------------------------------------------------------------------------------------------------
    super(OutputStream, self).__init__()
    self._signals = [ ]
    repo = Repository(recording)
    rec = repo.get_recording(recording)
    logging.debug("got recording: %s %s", type(rec), str(rec.uri))
    for s in signals:
      self._signals.append(repo.get_signal(s))
    rec.close()
    repo.close()
    logging.debug("got signals: %s", [ (type(s), str(s.uri)) for s in self._signals ])
    self._units = units
    self._rate = rate
    self._dtypes = dtypes
    self._segment = segment
    self._nometadata = nometadata
    self._pipe = pipe
    self._binary = binary

  def run(self):
  #-------------

    def send_data(fd, data):
    #-----------------------
      pos = 0
      while pos < len(data):
        ready = select.select([], [fd], [], 0.5)
        if len(ready[1]) == 0: continue
        os.write(fd, data[pos:pos+select.PIPE_BUF])
        pos += select.PIPE_BUF

    output = framestream.FrameStream(len(self._signals), self._nometadata, self._binary)
    ratechecker = RateChecker(self._rate)
    readers = [ ]
    if self._pipe == 'stdout':
      fd = sys.stdout.fileno()
      fifo = False
    else:
      fd = os.open(self._pipe, os.O_WRONLY | os.O_NONBLOCK)
      fifo = True
    for n, s in enumerate(self._signals):
      readers.append(SignalReader(s, output, n, ratechecker,
                                  rate=self._rate,
                                  units=self._units.get(n, self._units.get(-1)),
                                  dtype=self._dtypes.get(n, self._dtypes.get(-1)),
                                  interval=self._segment, maxpoints=BUFFER_SIZE))
    try:
      for r in readers: r.start()
      for frame in output.frames():
        if _interrupted.is_set(): break
        send_data(fd, frame)
        if not self._binary: send_data(fd, '\n')
        os.fsync(fd)   ## Does this slowdown stdout ??
    except Exception, err:
      logging.debug("ERROR: %s", err)
    finally:
      logging.debug("Finishing...")
      for r in readers:
        if r.is_alive(): r.terminate()
      if fifo:
        os.close(fd)
        logging.debug("Closed output pipe...")


class InputStream(Process):
#==========================

  def __init__(self, recording, signals, units, rate, dtypes, pipe, binary=False):
  #-------------------------------------------------------------------------------
    super(InputStream, self).__init__()
    self._rate = rate
    self._dtypes = dtypes
    self._pipe = pipe
    self._binary = binary
    self._repo = Repository(recording)
    self._recording = self._repo.new_recording(recording)  ##, description=, )
    self._signals = [ ]
    for n, s in enumerate(signals):
      try:
        rec = self._repo.get_recording(s)
        if s != rec.uri: raise ValueError("Resource <%s> already in repository" % s)
      except IOError:
        pass
      self._signals.append(self._recording.new_signal(s, units.get(n, units.get(-1)), rate=rate))


  def run(self):
  #-------------

    def newdata(n):
    #--------------
      return [ [] for i in xrange(n) ] # Independent lists

    def writedata(signals, data):
    #----------------------------
      for n, s in enumerate(signals):
        s.append(data[n], dtype=self._dtypes.get(n, self._dtypes.get(-1)))

    count = 0
    frames = 0
    channels = len(self._signals)
    data = newdata(channels)
    if self._pipe == 'stdin':
      fd = sys.stdin.fileno()
    else:
      fd = os.open(self._pipe, os.O_RDONLY | os.O_NONBLOCK)

#    for l in self._infile:      ### Binary.... ???
    buf = ''
    while True:
      ready = select.select([fd], [], [], 0.5)
      if len(ready[0]) == 0: continue
      indata = os.read(fd, 1024)
      if indata == '': break
      buf += indata
      lines = buf.split('\n')
      buf = lines.pop(-1)
      for l in lines:
        frames += 1
        for n, d in enumerate(l.split()[1:]):
          data[n].append(float(d))
        count += 1
        if count >= BUFFER_SIZE:
          writedata(self._signals, data)
          data = newdata(channels)
          count = 0
    logging.debug("Got %d frames", frames)
    os.close(fd)
    if count > 0: writedata(self._signals, data)
    self._recording.duration = frames/self._rate
    self._recording.close()
    self._repo.close()


def stream_data(connections):
#============================

  def get_units(units):
  #--------------------
    if   units in [None, '']: return None
    elif units[0] == '<':     return units[1:-1]
    else:                     return get_units_uri(units)

  def get_interval(segment):
  #-------------------------
    if segment is None:
      return None
    times = [segment[0], segment[2]]
    if segment[1] == ':':
      ## ISO durations.... OR seconds...
      return tuple(times)
    elif segment[1] == '-':
      if times[1] >= times[0]: raise ValueError("Duration can't be negative")
      return [ times[0], times[1] - times[0] ]

  def create_pipe(name):
  #---------------------
    if name in ['stdin', 'stdout']:
      return name
    pipe = os.path.abspath(name)
    try:
      os.makedirs(os.path.dirname(pipe))
      os.mkfifo(pipe, 0600)
    except OSError, e:
      if e.errno == errno.EEXIST: pass
      else: raise
    return pipe


  streams = [ ]
  dtypes = { -1: 'f4' }   ## Don't allow user to specify

  for defn in language.parse(testdef):

    if   defn[0] == 'stream':
      recording = defn[1][0][1:-1]
      base = recording + '/'
      pipe = create_pipe(defn[1][1])
      options = dict(defn[1][2:])
      rate = options.get('rate', None)
      units = { -1: get_units(options.get('units', None)) }
      segment = get_interval(options.get('segment', None))
      metadata = options.get('metadata', False)
      binary = options.get('binary', False)
      signals = [ ]
      for n, sig in enumerate(defn[2]):
        signals.append(urlparse.urljoin(base, sig[0][1:-1]))
        if len(sig) > 1 and sig[1][0] == 'units':
          units[n] = get_units(sig[1][1])
      streams.append(OutputStream(recording, signals, units, rate, dtypes, segment, not metadata, pipe, binary))

    elif defn[0] == 'recording':
      recording = defn[1][0][1:-1]
      base = recording + '/'
      pipe = create_pipe(defn[1][1])
      options = dict(defn[1][2:])
      rate = options.get('rate', None)
      if rate is None: raise ValueError("Input rate must be specified")
      units = { -1: get_units(options.get('units', None)) }
      binary = options.get('binary', False)
      signals = [ ]
      for n, sig in enumerate(defn[2]):
        signals.append(urlparse.urljoin(base, sig[0][1:-1]))
        if len(sig) > 1 and sig[1][0] == 'units':
          units[n] = get_units(sig[1][1])
      streams.append(InputStream(recording, signals, units, rate, dtypes, pipe, binary))

  sighandler.signal(sighandler.SIGINT, interrupt)
  try:
    for s in streams: s.start()
  except Exception, msg:
    _interrupted.set()
    return msg
  finally:
    for s in streams:
#      print s, s.is_alive(), s.pid, s.exitcode
      if s.is_alive(): s.join(0.5)


if __name__ == '__main__':
#=========================

  multiprocessing.freeze_support()
  # We lock up with ^C interrupt unless multiprocessing has a logger
  logger = multiprocessing.log_to_stderr()
  logger.setLevel(logging.ERROR)

  LOGFORMAT = '%(asctime)s %(levelname)8s %(processName)s: %(message)s'
  logging.basicConfig(format=LOGFORMAT)
  #if args['--debug']:
  logging.getLogger().setLevel(logging.DEBUG)

  testdef = """
    stream <http://devel.biosignalml.org/testdata/sinewave>
      to temp/pipe
      rate=10000
      segment = 0:1
        [ <signal/0> units=mV ]
    recording <http://devel.biosignalml.org/testdata/stream2>
      from temp/pipe
      rate=100
        [ <s1> units=mV ]
    """

#      to /temp/pipe
#      segment = 1:0.5

# stream
#   <http://devel.biosignalml.org/testdata/sinewave> /tmp/pipe
#     {'units': 'mV', 'binary': 'yes', 'rate': 100.0, 'segment': ([10.0, ':', 5.0], {})}
#     <signal/0> {}
# stream
#   <http://devel.biosignalml.org/testdata/sinewave> -
#     {'segment': ([10.0, '-', 20.7], {}), 'metadata': 'no'}
#     <signal/0> {'units': '<http://www.sbpax.org/uome/list.owl#Millivolt>'}
#     <signal/0> {'units': 'mV'}
# recording
#   <http://devel.biosignalml.org/testdata/sinewave> /tmp/pipe
#     {'rate': 3.0}
#     <signal/0> {}
# recording
#   <http://devel.biosignalml.org/testdata/sinewave2> -
#     {'units': '<my/units>'}
#     <signal/0> {'units': 'mV', 'rate': 10.0}
#     <s3> {}

  sys.exit(stream_data(testdef))
