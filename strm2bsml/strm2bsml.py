import numpy as np

from biosignalml.client import Repository
from biosignalml.data import UniformTimeSeries
import biosignalml.units as units
import biosignalml.rdf as rdf


VERSION = '0.1'

BUFFER_SIZE = 50000


if __name__ == '__main__':
#=========================

  import os, sys

  USAGE = 'Usage: %s [options] RECORDING_URI RATE ([-p POS] [-u UNITS] SIGNAL_ID)+' % sys.argv[0]

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
    if len(args) < 4: error_exit()

    parameters = { }

    n = 1
    if args[n] == '-d':
      n += 1
      parameters['delimiter'] = args
      n += 1
    if not args[n].startswith('http://'):
      error_exit('Invalid recording URI')
    parameters['recording'] = args[n]
    n += 1
    try: parameters['rate'] = float(args[n])
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
          unit = units.get_units_uri(u)
          if unit is None: error_exit('Unknown units: %s' % u)
          unit = str(unit)
        else:
          unit = u
        signals.append((pos, unit, args[n]))
        pos += 1
        n += 1
    except IndexError:
      error_exit('Missing signal argument')
    parameters['signals'] = signals
    return parameters


  args = parse_args(sys.argv)

  uri = args['recording']
  rate = args['rate']
  signals = args['signals']

  repo = Repository(uri)
  rec = repo.new_recording(uri)  ##, description=, )
  signals = [ ]
  for n, s in enumerate(args['signals']):
    signals.append(rec.new_signal(None, s[1], id=s[2], rate=rate))

  frames = 0
  count = 0
  data = [ [] for i in xrange(len(signals)) ] # Independent lists
  #      [ [] ]*len(signals)                  # Shared list object...
  rdfxml = [ ]
  for l in sys.stdin:
    if (len(rdfxml) > 0
     or l.startswith('<?xml ')
     or l.startswith('<rdf:RDF ')):
      rdfxml.append(l)
    elif l.startswith('</rdf:RDF>'):
      rdfxml.append(l)
      rec.save_metadata(''.join(rdfxml), rdf.Format.RDFXML)
      rdfxml = []
    else:
      frames += 1
      d = l.split(args.get('delimiter'))
      for n, s in enumerate(args['signals']):
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
