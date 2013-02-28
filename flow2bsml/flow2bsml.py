import mmap
import struct
import os
import uuid
import urllib
import platform
from datetime import datetime
import logging


from biosignalml import BSML
from biosignalml.client import Repository
from biosignalml.data import UniformTimeSeries
import biosignalml.model as model
import biosignalml.units as units
import biosignalml.rdf as rdf


__version__ = '0.4.0'


def get_time(ts):
#================
  buf = buffer(ts)
  date = struct.unpack_from("<H", buf)[0]
  year = 2000 + (date >> 9)
  month = (date & 0x01E0) >> 5
  day = (date & 0x001F)
  time = struct.unpack_from("<H", buf, 2)[0]
  hours = time >> 11
  mins = (time & 0x07E0) >> 5
  secs = (time & 0x001F) << 1
  if (1 <= month <= 12 and 1 <= day <= 31
   and hours < 24 and mins < 60 and secs < 60):
    return datetime(year, month, day, hours, mins, secs)
  else:
    raise ValueError("Invalid timestamp")


def good_block(b):
#=================
  return b[103:105] == '\xFF\xFF'


class SendError(Exception):
#==========================
  pass


def send_file(repo, base, fn, uid=False):
#========================================

  logging.debug("Converting %s", fn)

  fn = os.path.normpath(fn)
  f = open(fn, mode='r')
  buf = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
  hdr = buf[:512]
  if hdr[-1] != '\x3B': raise SendError("Bad file header")
  header = hdr.split('\x0D')
  if len(header) != 7: raise SendError("Wrong header format")
  if   buf[512:516] == '\x00\x00\x00\x00':
    raise SendError("File appears to be empty")
  error = ''
  try:
    if not good_block(buf[516:]): raise ValueError("Block error")
    timestamp = get_time(buf[512:516])
    pos = 516
  except ValueError:
    if good_block(buf[1028:]):
      error = "First 512-byte chunk corrupt"
      timestamp = get_time(buf[1024:1028])
      pos = 1028
    else:
      error = "Short first block"
      timestamp = get_time(buf[512:516])
      pos = buf.find('\xFF\xFF') + 2
  serialno = header[3]
  fileid = header[2].rsplit('.', 1)[0]

  if uid: uri = base + '/' + str(uuid.uuid4())
  else:
    path = os.path.splitext(fn)[0].replace(' ', '_') # Remove extension and replace ' ' with '_'
    if path[0] not in ['.', '/']: uri = '/' + path
    elif path.startswith('./'):   uri = path[1:]
    else:                         uri = os.path.abspath(path)
    if '-' in uri or uri != urllib.quote(uri):
      raise SendError("Invalid characters in resulting URI")
    uri = base + uri
  try:
    repo.get_recording(uri)
    # if not replacing:
    raise SendError("Recording `%s` already exists" % uri)
  except IOError:
    pass
  logging.info('%s --> %s', fn, uri)

  rec = repo.new_recording(uri, ## description=,
                           starttime=timestamp, duration=0.0,
                           source='file://%s%s' % (platform.node(), os.path.realpath(fn)),
                           creator='http://devices.biosignalml.org/icon/%s' % serialno)
  if error: rec.associate(model.Annotation.Note(rec.uri.make_uri(), rec.uri,
                 error, tags=[BSML.ErrorTAG],
                 creator='file://' + os.path.abspath(__file__)))

  # rec.annotate(error, tags=, ...)  ####
  ## Graph is only created when getting metadata as graph...
  ## So core abstract object can have a list of annotations and an 'annotate' method??

  ## Also device model, firmware rev, serial number, etc...

  flow = rec.new_signal(None, units=units.get_units_uri('lpm'),
    id=0, rate=50, label='Flow', dtype='f4')
  pressure = rec.new_signal(None, units=units.get_units_uri('cmH2O'),
    id=1, rate=1,  label='CPAP Pressure', dtype='f4')
  leak = rec.new_signal(None, units=units.get_units_uri('lpm'),
    id=2, rate=1,  label='Leak', dtype='f4')
  fdata = []
  pdata = []
  ldata = []
  duration = 0
  logging.debug("Reading file...")
  while buf[pos:pos+4] != '\xFF\x7F\x00\x00':
    record = buffer(buf[pos:pos+105])
    duration += 1
    for i in xrange(50):
      fdata.append(struct.unpack_from('<h', record, 2*i)[0]/100.0)
    pdata.append(struct.unpack_from('<h', record, 100)[0]/100.0)
    ldata.append(struct.unpack_from('B', record, 102)[0]/100.0)
    if record[103:105] != '\xFF\xFF':
      raise SendError("Remainder of file has a short block")
    pos += 105

  logging.debug("All read, starting append...")
  flow.append(UniformTimeSeries(fdata, rate=50))
  ## sent as 'f8' ??

  logging.debug("Flow appended...")
  pressure.append(UniformTimeSeries(pdata, rate=1))
  leak.append(UniformTimeSeries(ldata, rate=1))

  logging.debug("Finishing...")

  rec.duration = duration    ### Does this update metadata on server...??
  rec.close()

  buf.close()
  f.close()


if __name__ == '__main__':
#=========================

  import urlparse
  import docopt
  import sys


  LOGFORMAT = '%(asctime)s %(levelname)8s: %(message)s'
  logging.basicConfig(format=LOGFORMAT)
  logging.getLogger().setLevel(logging.INFO)
  logging.info('%s: Version %s', sys.argv[0], __version__)

  usage = """Usage:
  %(prog)s [options] REPO FILE...
  %(prog)s (-h | --help)

Store Flow files in a BioSignalML repository.

REPO is a URI, specifying both the repository and a prefix to use when
constructing URIs for stored files (ie. Recordings).

Recording URI's are formed by suffixing REPO with either a FILE path (with
spaces replaced by underscores), or with a unique UUID string.

Options:

  -h --help     Show this text and exit.

  -u --uuid     Use UUID strings for file names.

  """

  ## As parameters....
  # ./FlowData/NZ_Patients/30_Day_Data/07124524/FLW0003.FPH
##  filepath = fn.rsplit('/FlowData/', 1)[1].rsplit('/', 1)[0].replace(' ', '_')
##  p = filepath.split('/')
##  region = p[0]
##  trial = p[1]


  args = docopt.docopt(usage % { 'prog': sys.argv[0] } )
  base = args['REPO']
  p = urlparse.urlparse(base)
  repo = Repository(p.scheme + '://' + p.netloc)
  if base.endswith('/'): base = base[:-1]
  try:
    for f in args['FILE']:
      try:
        send_file(repo, base, f, args['--uuid'])
      except SendError, msg:
        logging.error('%s: %s', f, msg)
  finally:
    repo.close()

