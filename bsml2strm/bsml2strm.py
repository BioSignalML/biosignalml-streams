import sys
import signal as sighandler
import threading
import logging

from biosignalml.client import Repository
from biosignalml.units import get_units_uri

import framestream

VERSION = '0.4.0'

BUFFER_SIZE = 10000

##Stream signals at the given RATE.
##Otherwise all URIs must be for signals from the one BioSignalML recording.


_thread_exit = threading.Event()

class SignalReader(threading.Thread):
#====================================

  def __init__(self, signal, output, channel, ratechecker, **options):
  #-------------------------------------------------------------------
    threading.Thread.__init__(self)
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
        if _thread_exit.is_set(): break
        if ts.is_uniform:
          self._ratechecker.check(ts.rate)
          self._output.put_data(self._channel, ts.data)
        else:
          self._ratechecker.check(None)
          self._output.put_data(self._channel, ts.points)
    finally:
      self._output.put_data(self._channel, None)
      logging.debug("Finished channel %d", self._channel)


def interrupt(signum, frame):
#============================
  _thread_exit.set()
  sys.exit()


class RateChecker(object):
#=========================

  def __init__(self):
  #------------------
    self._lock = threading.Lock()
    self._rate = -1

  def check(self, rate):
  #---------------------
    if self._rate == -1:
      self._lock.acquire()
      self._rate = rate
      self._lock.release()
    elif self._rate != rate:
      _thread_exit.set()
      raise ValueError("Signal rates don't match")


def bsml2strm(uris, units, rate, dtypes, segment, nometadata, outfile, binary=False):
#====================================================================================

  signals = [ ]
  for u in uris:
    repo = Repository(u)
    rec = repo.get_recording(u)
    logging.debug("got recording: %s %s", type(rec), str(rec.uri))
    if u == str(rec.uri): signals.extend([ s for s in rec.signals() if s.rate is not None ])
    else:                 signals.append(repo.get_signal(u))
    repo.close()

  logging.debug("got signals: %s", [ (type(s), str(s.uri)) for s in signals ])

#  rate = signals[0].rate    ############
#  for s in signals[1:]:
#    if rate != s.rate:
#      raise NotImplementedError("Rate conversion not yet implemented")


  output = framestream.FrameStream(len(signals), nometadata, binary)
  sighandler.signal(sighandler.SIGINT, interrupt)
  ratechecker = RateChecker()
  readers = [ ]
  try:
    for n, s in enumerate(signals):
      readers.append(SignalReader(s, output, n, ratechecker,
                                  rate=rate,
                                  units=units.get(n, units.get(-1)),
                                  dtype=dtypes.get(n, dtypes.get(-1)),
                                  interval=segment, maxpoints=BUFFER_SIZE))
      readers[-1].start()                   # Start thread

    for f in output.frames():
      outfile.write(f)
      if not binary: outfile.write('\n')
      # Calling flush() significantly slows throughput...

  finally:
    for t in readers:
      if t.is_alive(): t.join()


if __name__ == '__main__':
#=========================

  import urlparse
  import docopt
  import pyparsing as pp
  import numpy as np


  LOGFORMAT = '%(asctime)s %(levelname)8s %(threadName)s: %(message)s'
  logging.basicConfig(format=LOGFORMAT)
##  logging.getLogger().setLevel(logging.DEBUG)
  logging.debug("Starting...")


  usage = """Usage:
  %(prog)s [options] [-u UNITS --units=UNITS] URI...
  %(prog)s (-h | --help)

Channel order is that of the given URIs. If the URI is that of a recording then all
signals in the recording are streamed.

All signals MUST have the same sampling rate.

Each line in a text output stream starts with a frame number, followed by
space-separated channel values, with the last channel being metadata. A binary
output stream is a sequence of 32-bit floating point values, with no frame
number.


Options:

  -h --help   Show this text and exit.

  -b BASE --base=BASE            Base prefix for URIs

  --binary                       Output data as 32-bit floats.

  --no-metadata                  Don't add a metadata channel.

  -r RATE --rate RATE            Stream signals at the given RATE.

  -s SEGMENT --segment=SEGMENT   Temporal segment of recording to stream.

              SEGMENT is either "start-end" or "start:duration", with times being
              ISO 8601 durations (e.g. "PT1M23.5S"). Start and end times are from
              the beginning of the recording; a missing start time means "PT0S";
              a missing end time means the recording's end; and a missing duration
              means until the recording's end.

  -u UNITS --units=UNITS         A comma separated list of "N:unit"
              entries, where "N" is the 0-origin channel number and "unit" is
              either an abbreviation for a unit, a QNAME (i.e. prefix:name), or
              a full URI enclosed in angle brackets;

              or UNITS is in the form "@file", where the named "file" contains
              unit specifications as a comma and/or line separated list.

              When specified, units are checked and, if possible, data is converted.

              A default setting (for all channels) can be given by an entry
              which has no channel number (i.e. without the "N:" prefix).

"""

  """
  -d TYPES --dtypes TYPES        A comma separated list of "N:type"
              entries, where "N" is the 0-origin channel number and "type" is
              a string, in NumPy's array protocol format, giving the numeric
              type channel data will be streamed from the host as. (The first
              character specifies the kind of data (e.g. 'i' = integer, 'f' =
              float) and the remaining characters specify how many bytes of data.
              See: http://docs.scipy.org/doc/numpy/reference/arrays.dtypes.html).

              A default setting (for all channels) can be given by an entry
              which has no channel number (i.e. without the "N:" prefix).

              [Default: f4] (32-bit float)
  """

  ## PyParsing grammer for option value lists.
  opt_value = pp.CharsNotIn(' ,')
  opt_channel = pp.Word(pp.nums).setParseAction(lambda s,l,t: [int(t[0])])
  opt_chanvalue = pp.Group(pp.Optional(opt_channel + pp.Suppress(':'), default=-1) + opt_value)
  opt_valuelist = pp.delimitedList(opt_chanvalue, delim=',')

  def parse_rate(rate):
  #====================
    if rate in [None, '']: return
    try:
      return float(rate)
    except ValueError:
      raise ValueError("Invalid rate")

  def parse_units(units):
  #======================
    result = { }
    for u in units:
      if u.startswith('@'):
        with open(u[1:]) as file:
          result.update(parse_units(file.read().split()))
      else:
        for l in opt_valuelist.parseString(u):
          try:
            uri = l[1]
            if uri.startswith('http://'): result[l[0]] = uri
            else:                         result[l[0]] = get_units_uri(uri)
          except ValueError as e:
            raise ValueError("Invalid units specification - %s" % e)
    return result

  def parse_dtypes(dtypes):
  #========================
    result = { }
    if dtypes is not None:
      for l in opt_valuelist.parseString(dtypes):
        try:
          result[l[0]] = np.dtype(l[1]).str
        except (IndexError, ValueError) as e:
          raise ValueError("Invalid datatype - %s" % e)
    return result

  def parse_segment(segment):
  #==========================
    if segment in [None, '']:
      return
    elif ':' in segment:

      ## ISO durations.... OR seconds...

      return [ float(t) for t in segment.split(':') ]
    elif '-' in segment:

      t = [ float(t) for t in segment.split('-') ]

      return ( t[0], t[1] - t[0] )
    elif segment:
      raise ValueError("Invalid segment specification")

  def add_base(base, uri):
  #-----------------------
    if base is None or uri is None or uri.startswith('http:'):
      return uri
    else:
      return urlparse.urljoin(base, uri)


  args = docopt.docopt(usage % { 'prog': sys.argv[0] } )
#  rate = float(args['RATE'])
  units = parse_units(args['--units'])
  ##dtypes = parse_dtypes(args['--dtypes'])
  dtypes = { -1: 'f4' }   ## Don't allow user to specify
  segment = parse_segment(args['--segment'])
  base = args['--base']
  uris = [ add_base(base, u) for u in args['URI'] ]

  try:
    bsml2strm(uris, units, parse_rate(args['--rate']), dtypes, segment,
                    args['--no-metadata'], sys.stdout, args['--binary'])
  except Exception, msg:
    sys.exit(msg)
