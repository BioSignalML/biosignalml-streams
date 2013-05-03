import os, sys
import errno
import select
import logging
import urlparse
import multiprocessing
import multiprocessing.sharedctypes
import signal as sighandler

from biosignalml.client import Repository
from biosignalml.units import get_units_uri

import language
import framestream

VERSION = '0.5.0'

BUFFER_SIZE = 10000

##Stream signals at the given RATE.
##Otherwise all URIs must be for signals from the one BioSignalML recording.


_interrupted = multiprocessing.Event()

def interrupt(signum, frame):
#============================
  _interrupted.set()


def get_units(units, default=None):
#----------------------------------
  if   units in [None, '']: return default
  elif units[0] == '<':     return units[1:-1]
  else:                     return get_units_uri(units)


class SynchroniseCondition(object):
#==================================

  def __init__(self):
  #------------------
    self._condition = multiprocessing.Condition()
    self._count = multiprocessing.sharedctypes.Value('i', 0)

  def add_waiter(self):
  #--------------------
    self._condition.acquire()
    self._count.value += 1
#    logging.debug('Waiters: %d', self._count.value)
    self._condition.release()

  def wait_for_everyone(self):
  #---------------------------
    self._condition.acquire()
    if self._count.value > 0: self._count.value -= 1
#    logging.debug('Waiting: %d', self._count.value)
    self._condition.notify_all()
    while self._count.value > 0:
      self._condition.wait()
#    logging.debug('Running: %d', self._count.value)
    self._condition.release()


_sender_lock = SynchroniseCondition()


class SignalReader(multiprocessing.Process):
#===========================================

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
    self._lock = multiprocessing.Lock()
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


class OutputStream(multiprocessing.Process):
#===========================================

  def __init__(self, recording, signals, dtypes, segment, nometadata, pipe, binary=False):
  #---------------------------------------------------------------------------------------
    super(OutputStream, self).__init__()
    rec_uri = recording[0]
    options = recording[1]
    rate = options.get('rate')
    units = { -1: get_units(options.get('units')) }
    self._signals = [ ]
    repo = Repository(rec_uri)
    rec = repo.get_recording(rec_uri)
    logging.debug("got recording: %s %s", type(rec), str(rec.uri))
    for n, s in enumerate(signals):
      self._signals.append(repo.get_signal(s[0]))
      units[n] = get_units(s[1].get('units'))
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
      fd = os.open(self._pipe, os.O_RDWR)
      fifo = True
    for n, s in enumerate(self._signals):
      readers.append(SignalReader(s, output, n, ratechecker,
                                  rate=self._rate,
                                  units=self._units.get(n, self._units.get(-1)),
                                  dtype=self._dtypes.get(n, self._dtypes.get(-1)),
                                  interval=self._segment, maxpoints=BUFFER_SIZE))
    try:
      for r in readers: r.start()
      starting = True
      for frame in output.frames():
        if starting:
          _sender_lock.wait_for_everyone()
          starting = False
        if _interrupted.is_set(): break
        send_data(fd, frame)
        if not self._binary: send_data(fd, '\n')
        os.fsync(fd)   ## Does this slowdown stdout ??
    except Exception, err:
      logging.debug("ERROR: %s", err)
    finally:
      for r in readers:
        if r.is_alive(): r.terminate()
      if fifo: os.close(fd)  # Don't close stdout
      logging.debug("Finished output: %s", self._pipe)


class InputStream(multiprocessing.Process):
#==========================================

  def __init__(self, recording, signals, dtypes, pipe, binary=False):
  #------------------------------------------------------------------
    super(InputStream, self).__init__()
    rec_uri = recording[0]
    options = recording[1]
    rate = options.get('rate')
    if rate is None: raise ValueError("Input rate must be specified")
    units = { -1: get_units(options.get('units')) }
    self._rate = rate
    self._dtypes = dtypes
    self._pipe = pipe
    self._binary = binary
    self._repo = Repository(rec_uri)
    kwds = dict(label=options.get('label'), description=options.get('desc'))
    self._recording = self._repo.new_recording(rec_uri, **kwds)
    self._signals = [ ]
    for s in signals:
      sig_uri = s[0]
      sigopts = s[1]
      try:
        rec = self._repo.get_recording(sig_uri)
        if rec_uri != rec.uri: raise ValueError("Resource <%s> already in repository" % sig_uri)
      except IOError:
        pass
      kwds = dict(rate=rate, label=sigopts.get('label'), description=sigopts.get('desc'))
      self._signals.append(self._recording.new_signal(sig_uri,
                                                      get_units(sigopts.get('units'), units),
                                                      **kwds))

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
    try: os.makedirs(os.path.dirname(pipe))
    except OSError, e:
      if e.errno == errno.EEXIST: pass
      else:                       raise
    try: os.mkfifo(pipe, 0600)
    except OSError, e:
      if e.errno == errno.EEXIST: pass
      else:                       raise
    return pipe

  try:
    definitions = language.parse(connections)
  except ValueError, msg:
    return msg

  streams = [ ]
  dtypes = { -1: 'f4' }   ## Don't allow user to specify
  for defn in definitions:
    if   defn[0] == 'stream':
      recording = defn[1][0][1:-1]
      base = recording + '/'
      pipe = create_pipe(defn[1][1])
      options = dict(defn[1][2:])
      segment = get_interval(options.pop('segment', None))
      metadata = options.pop('metadata', False)
      binary = options.pop('binary', False)
      signals = [ (urlparse.urljoin(base, sig[0][1:-1]), dict(sig[1:])) for sig in defn[2]]
      streams.append(OutputStream((recording, options), signals, dtypes, segment, not metadata, pipe, binary))
      _sender_lock.add_waiter()
    elif defn[0] == 'recording':
      recording = defn[1][0][1:-1]
      base = recording + '/'
      pipe = create_pipe(defn[1][1])
      options = dict(defn[1][2:])
      binary = options.pop('binary', False)
      signals = [ (urlparse.urljoin(base, sig[0][1:-1]), dict(sig[1:])) for sig in defn[2]]
      streams.append(InputStream((recording, options), signals, dtypes, pipe, binary))

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

  import docopt

  multiprocessing.freeze_support()
  # We lock up with ^C interrupt unless multiprocessing has a logger
  logger = multiprocessing.log_to_stderr()
  logger.setLevel(logging.ERROR)

  LOGFORMAT = '%(asctime)s %(levelname)8s %(processName)s: %(message)s'
  logging.basicConfig(format=LOGFORMAT)

  usage = """Usage:
  %(prog)s [options] (CONNECTION_DEFINITION | -f FILE)
  %(prog)s (-h | --help)

Connect a BioSignalML repository with telemetry streams,
using definitions from either the command line or a file.

Options:

  -h --help      Show this text and exit.

  -f FILE --file FILE
                 Take connection information from FILE.

  -d --debug     Enable debugging.

  """

  args = docopt.docopt(usage % { 'prog': sys.argv[0] } )

  if args['--debug']: logging.getLogger().setLevel(logging.DEBUG)
  logging.debug("ARGS: %s", args)

  if args['--file'] is not None:
    with open(args['--file']) as f:
      definitions = f.read()
  elif args['CONNECTION_DEFINITION'] is not None:
    definitions = args['CONNECTION_DEFINITION']

  sys.exit(stream_data(definitions))
