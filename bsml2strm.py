import sys
import signal as sighandler
import urlparse
import threading
import numpy as np
import logging

import docopt

from biosignalml.client import Repository
from biosignalml.units import get_units_uri

import framestream

VERSION = '0.2'

BUFFER_SIZE = 10000


usage = """Usage:
  %(prog)s [options] [-u UNITS --units=UNITS] RATE (RECORDING_URI | SIGNAL_URI ...)
  %(prog)s (-h | --help)

Stream signals at the given RATE. Channel order is that of the given URIs.

If the URI is that of a recording then all signals in the recording are streamed.
Otherwise all URIs must be for signals from the one BioSignalML recording.

Options:

  -h --help   Show this text and exit.

  -b BASE --base=BASE            Base prefix for URIs

  -d TYPES --dtypes TYPES        A comma or space separated list of "N=type"
              entries, where "N" is the 0-origin channel number and "type" is
              a string, in NumPy's array protocol format, giving the numeric
              type channel data will be streamed from the host as. (The first
              character specifies the kind of data (e.g. 'i' = integer, 'f' =
              float) and the remaining characters specify how many bytes of data.
              See: http://docs.scipy.org/doc/numpy/reference/arrays.dtypes.html).

              Default is to use the signal files's native datatype.

  --no-metadata                  Don't add a metadata channel.

  -s SEGMENT --segment=SEGMENT   Temporal segment of recording to stream.
  
              SEGMENT is either "start-end" or "start:duration", with times being
              ISO 8601 durations (e.g. "PT1M23.5S"). Start and end times are from
              the beginning of the recording; a missing start time means "PT0S";
              a missing end time means the recording's end; and a missing duration
              means until the recording's end.

  -u UNITS --units=UNITS         Required units for data channels.
  
              UNITS is either a comma (or space) separated list of "N=unit"
              entries, where "N" is the 0-origin channel number and "unit" is
              either an abbreviation for a unit, a QNAME (i.e. prefix:name), or
              a full URI enclosed in angle brackets;

              or UNITS is in the form "@file", where the named "file" contains
              unit specifications as a comma, space, or line separated list.

              When specified, units are checked and, if possible, data is converted.

"""

def parse_units(units):
#======================
  result = { }
  for u in units:
    if u.startswith('@'):
      with open(u[1:]) as file:
        result.update(parse_units(file.read().split()))
    else:
      for s in u.split():       # Could combine using list comprehension
        for t in s.split(','):  # and reduce(), but this is clearer...
          l = t.split('=', 1)
          try:
            uri = l[1].strip()
            if uri.startswith('http://'): result[int(l[0])] = uri
            else:                         result[int(l[0])] = get_units_uri(uri)
          except (IndexError, ValueError) as e:
            raise ValueError("Invalid units specification - %s" % e)
  return result


def parse_dtypes(dtypes):
#========================
  result = { }
  if dtypes is not None:
    for d in dtypes.split():  # Could combine using list comprehension
      for t in d.split(','):  # and reduce(), but this is clearer...
        l = t.split('=', 1)
        try:
          result[int(l[0])] = np.dtype(l[1].strip()).str
        except (IndexError, ValueError, TypeError) as e:
          raise ValueError("Invalid datatype - %s" % e)
  return result


def parse_segment(segment):
#==========================
  if segment in [None, '']:
    return
  elif ':' in segment:
    return [ float(t) for t in segment.split(':') ]
  elif '-' in segment:
    t = [ float(t) for t in segment.split('-') ]
    return ( t[0], t[1] - t[0] )
  elif segment:
    raise ValueError("Invalid segment specification")


_thread_exit = threading.Event()

class SignalReader(threading.Thread):
#====================================

  def __init__(self, signal, output, channel, **options):
  #------------------------------------------------------
    threading.Thread.__init__(self)
    self._signal = signal
    self._output = output
    self._channel = channel
    self._options = options

  def run(self):
  #-------------
    try:
      for ts in self._signal.read(**self._options):
        if _thread_exit.is_set(): break
        if ts.is_uniform:
          self._output.put_data(self._channel, ts.data)
        else:
          self._output.put_data(self._channel, ts.points)
    finally:
      self._output.put_data(self._channel, None)


def interrupt(signum, frame):
#============================
  _thread_exit.set()
  sys.exit()


if __name__ == '__main__':
#=========================

  LOGFORMAT = '%(asctime)s %(levelname)8s %(threadName)s: %(message)s'
  logging.basicConfig(format=LOGFORMAT)
  logging.getLogger().setLevel(logging.DEBUG)

  def add_base(base, uri):
  #-----------------------
    if base is None or uri is None or uri.startswith('http:'):
      return uri
    else:
      return urlparse.urljoin(base, uri)

  args = docopt.docopt(usage % { 'prog': sys.argv[0] } )
  rate = float(args['RATE'])
  units = parse_units(args['--units'])
  dtypes = parse_dtypes(args['--dtypes'])
  segment = parse_segment(args['--segment'])
  base = args['--base']
  rec_uri = add_base(base, args['RECORDING_URI'])
  sig_uris = [ add_base(base, s) for s in args['SIGNAL_URI'] ]

  if rec_uri is not None:
    repo = Repository.connect(rec_uri)
    try:
      rec = repo.get_recording_with_signals(rec_uri)
      signals = [ s for s in rec.signals() if s.rate is not None ]
    except IOError:
      signals = [ repo.get_signal(rec_uri) ]
  else:
    repo = Repository.connect(sig_uris[0])
    signals = [ ]
    for s in sig_uris:
      signal = repo.get_signal(s)
      if signal.rate is None:
        raise NotImplementedError("Streaming of non-uniform signals not yet implemented") 
      signals.append(signal)
    rec = signals[0].recording
    for s in signals[1:]:
      if rec != s.recording:
        raise ValueError("Signals are not all from the same recording")

  logging.debug("got signals: %s", signals)

  rate = signals[0].rate    ############
  for s in signals[1:]:
    if rate != s.rate:
      raise NotImplementedError("Rate conversion not yet implemented")

  output = framestream.FrameStream(len(signals), args['--no-metadata'])
  sighandler.signal(sighandler.SIGINT, interrupt)
  readers = [ ]
  try:
    for n, s in enumerate(signals):
      readers.append(SignalReader(s, output, n, units=units.get(n), dtype=dtypes.get(n),
                                  interval=segment, maxpoints=BUFFER_SIZE))
      readers[-1].start()                   # Start thread

    for f in output.frames():
      print f

  finally:
    for t in readers:
      if t.is_alive(): t.join()

  repo.close()
