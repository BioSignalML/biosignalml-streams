import urlparse
import numpy as np

from biosignalml.client import Repository
from biosignalml.data import UniformTimeSeries
import biosignalml.units as units

VERSION = '0.1'

BUFFER_SIZE = 50000


if __name__ == '__main__':
#=========================

  import os, sys

  USAGE = 'Usage: %s [-r REPOSITORY] RECORDING RATE ([-p POS] [-u units] SIGNAL)+' % sys.argv[0]

  ## [-c CHANNELS] # Or determine from first line of input ( = len(l.split()) - 1)

  def error_exit(msg=''):
  #----------------------
    if msg: sys.stderr.write('%s\n' % msg)
    sys.exit(USAGE)


  def parse_args(args):
  #--------------------
    if len(args) == 1 and args[0] == '-h':
      print USAGE
      print """ """
      sys.exit(0)
    if len(args) < 3: error_exit()
    n = 0
    repo = None
    if args[0] == '-r':
      repo = args[1]
      if not repo.startswith('http://'):
        error_exit('Invalid repository URI')
      n = 2
    rec_uri = args[n]
    if not rec_uri.startswith('http://'):
      error_exit('Invalid recording URI')
    n += 1
    try: rate = float(args[n])
    except ValueError: error_exit('Invalid rate')
    n += 1
    signals = [ ]
    pos = 1  ## Column 0 is frame count
    u = None
    try:
      while n < len(args):
        if args[n] == '-p':
          try: pos = int(args[n+1])
          except ValueError: error_exit('Invalid position')
          n += 2
        if args[n] == '-u':
          u = args[n+1]
          n += 2
        if u is not None and not u.startswith('http:'):
          unit = units.units(u)
          if unit is None: error_exit('Unknown units: %s' % u)
          unit = str(unit)
        else:
          unit = u
        signals.append((pos, unit, args[n]))
        pos += 1
        n += 1
    except IndexError:
      error_exit('Missing signal argument')
    return(repo, rec_uri, rate, signals)

  rep_uri, rec_uri, rate, sig_details = parse_args(sys.argv[1:])

  if rep_uri is None:
    p = urlparse.urlparse(rec_uri)
    rep_uri = p.scheme + '://' + p.netloc




##  print rep_uri, rec_uri, rate, sig_details  ######

  repo = Repository(rep_uri)
  rec = repo.new_recording(rec_uri)  ##, description=, )
  signals = [ ]
  for n, s in enumerate(sig_details):
    if s[2].startswith('http://'): (uri, id) = (s[2], None)
    else:                          (uri, id) = (None, s[2])
    signals.append(rec.new_signal(uri, s[1], id=id, rate=rate))


  frames = 0
  count = 0
  data = [ [] for i in xrange(len(signals)) ] # Independent lists
  #      [ [] ]*len(signals)                  # Shared list object...

  for l in sys.stdin:
    frames += 1
    d = l.split()
    for n, s in enumerate(sig_details):
      data[n].append(float(d[s[0]]))
    count += 1
    if count >= BUFFER_SIZE:
      for n, s in enumerate(signals): s.append(data[n])
      count = 0
      data = [ [] for i in xrange(len(signals)) ] # Independent lists
  if count > 0:
    for n, s in enumerate(signals):
#      print data[n]
      s.append(data[n])

  rec.duration = frames/rate
  rec.close()
  repo.close()

  print rec.graph
