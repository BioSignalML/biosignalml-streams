import mmap
import struct
import os
from datetime import datetime

from biosignalml import BSML
from biosignalml.formats import HDF5Recording
from biosignalml.data import UniformTimeSeries
import biosignalml.model as model
import biosignalml.units as units


URI_PREFIX   = "http://devel.biosignalml.org/fph/icon/"
DATA_PREFIX  = "/recordings/fph/icon/"


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


def convert(fn, replace=False):
#==============================

  print "Converting ", fn

  fn = os.path.abspath(fn)
  f = open(fn, mode='r')
  buf = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
  hdr = buf[:512]
  if hdr[-1] != '\x3B': raise TypeError("Bad file header")
  header = hdr.split('\x0D')
  if len(header) != 7: raise TypeError("Wrong header format")
  if   buf[512:516] == '\x00\x00\x00\x00':
    raise Exception("File appears to be empty")
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

  # ./FlowData/NZ_Patients/30_Day_Data/07124524/FLW0003.FPH
  filepath = fn.rsplit('/FlowData/', 1)[1].rsplit('/', 1)[0].replace(' ', '_')
  p = filepath.split('/')
  region = p[0]
  trial = p[1]

  datapath = DATA_PREFIX + filepath
  dataset = 'file://' + datapath + '/' + fileid + '.h5'
  uri = URI_PREFIX + filepath + '/' + fileid

  try:
    os.makedirs(datapath, 0755)
  except OSError:
    if os.path.isdir(datapath): pass
    else: raise # Directory creation error
  h5 = HDF5Recording.create(uri, dataset, replace=replace,
         starttime=timestamp, source='file://' + fn)
  h5.dataset = None     ## Don't store as metadata attribute

  if error: h5.associate(model.Annotation.Note(h5.uri.make_uri(), h5.uri,
                 error, tags=[BSML.ErrorTAG],
                 creator='file://' + os.path.abspath(__file__)))


  # h5.annotate(error, tags=, ...)  ####

  ## Graph is only created when getting metadata as graph...

  ## So core abstract object can have a list of annotations and an 'annotate' method??

  ## Also device model, firmware rev, serial number, etc...
  ## Also study details (from filename path...)
  flow = h5.new_signal(None, units=units.get_units_uri('lpm'),
    id=0, rate=50, label='Flow', dtype='f4')
  pressure = h5.new_signal(None, units=units.get_units_uri('cmH2O'),
    id=1, rate=1,  label='CPAP Pressure', dtype='f4')
  leak = h5.new_signal(None, units=units.get_units_uri('lpm'),
    id=2, rate=1,  label='Leak', dtype='f4')
  fdata = []
  pdata = []
  ldata = []
  duration = 0
  while buf[pos:pos+4] != '\xFF\x7F\x00\x00':
    rec = buffer(buf[pos:pos+105])
    duration += 1
    for i in xrange(50):
      fdata.append(struct.unpack_from('<h', rec, 2*i)[0]/100.0)
    pdata.append(struct.unpack_from('<h', rec, 100)[0]/100.0)
    ldata.append(struct.unpack_from('B', rec, 102)[0]/100.0)
    if rec[103:105] != '\xFF\xFF':
      raise Exception("Remainder of file has a short block")
    pos += 105
  flow.append(UniformTimeSeries(fdata, rate=50))
  pressure.append(UniformTimeSeries(pdata, rate=1))
  leak.append(UniformTimeSeries(ldata, rate=1))
  h5.duration = duration
  h5.save_metadata()
  h5.close()

  buf.close()
  f.close()


if __name__ == '__main__':
#=========================

  import sys

# Use argparse to add an --overwrite parameter...

#  fn = 'FlowData/US_Patients/7Day/07167919/FLW0003.FPH'
  if len(sys.argv) < 2:
    print 'Usage: %s file(s)' % sys.argv[0]
    sys.exit(1)

  for f in sys.argv[1:]:
    try: convert(f, True)      # One block == one second
    except Exception, msg:
      print '%s: %s' % (f, msg)
