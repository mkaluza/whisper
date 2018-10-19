#!/usr/bin/env python

import os
import mmap
import time
import struct
import signal
import sys
import optparse

try:
  import whisper
except ImportError:
  raise SystemExit('[ERROR] Please make sure whisper is installed properly')

if sys.version_info >= (3, 0):
  xrange = range

# Ignore SIGPIPE
signal.signal(signal.SIGPIPE, signal.SIG_DFL)

option_parser = optparse.OptionParser(usage='''%prog path''')
option_parser.add_option(
  '--pretty', default=False, action='store_true',
  help="Show human-readable timestamps instead of unix times")
option_parser.add_option(
  '-t', '--time-format', action='store', type='string', dest='time_format',
  help='Time format to use with --pretty; see time.strftime()')
(options, args) = option_parser.parse_args()

if len(args) != 1:
  option_parser.error("require one input file name")
else:
  path = args[0]


def mmap_file(filename):
  fd = os.open(filename, os.O_RDONLY)
  mm = mmap.mmap(fd, os.fstat(fd).st_size, prot=mmap.PROT_READ)
  os.close(fd)
  return mm


def read_header(mm):
  try:
    (aggregationType, maxRetention, xFilesFactor, archiveCount) \
      = struct.unpack(whisper.metadataFormat, mm[:whisper.metadataSize])
  except (struct.error, ValueError, TypeError):
    raise whisper.CorruptWhisperFile("Unable to unpack header")

  archives = []
  archiveOffset = whisper.metadataSize

  for i in xrange(archiveCount):
    try:
      (offset, secondsPerPoint, points, fmt) = struct.unpack(
        whisper.archiveInfoFormat,
        mm[archiveOffset:archiveOffset + whisper.archiveInfoSize]
      )
    except (struct.error, ValueError, TypeError):
      raise whisper.CorruptWhisperFile("Unable to read archive %d metadata" % i)

    parser = struct.Struct(fmt)
    archiveInfo = {
      'offset': offset,
      'secondsPerPoint': secondsPerPoint,
      'points': points,
      'retention': secondsPerPoint * points,
      'format': fmt,
      'parser': parser,
      'pointSize': parser.size,
      'size': points * parser.size,
    }
    archives.append(archiveInfo)
    archiveOffset += whisper.archiveInfoSize

  header = {
    'aggregationMethod': whisper.aggregationTypeToMethod.get(aggregationType, 'average'),
    'maxRetention': maxRetention,
    'xFilesFactor': xFilesFactor,
    'archives': archives,
  }
  return header


def dump_header(header):
  print('Meta data:')
  print('  aggregation method: %s' % header['aggregationMethod'])
  print('  max retention: %d' % header['maxRetention'])
  print('  xFilesFactor: %g' % header['xFilesFactor'])
  print("")
  dump_archive_headers(header['archives'])


def dump_archive_headers(archives):
  for i, archive in enumerate(archives):
    print('Archive %d info:' % i)
    print('  offset: %d' % archive['offset'])
    print('  seconds per point: %d' % archive['secondsPerPoint'])
    print('  points: %d' % archive['points'])
    print('  retention: %d' % archive['retention'])
    print('  format: %s' % archive['format'])
    print('  point size: %d' % archive['pointSize'])
    print('  size: %d' % archive['size'])
    print('  last timestamp: %d' % archive['lastTimestamp'])
    print('  last index: %d' % archive['lastIndex'])
    print("")


def dump_archives(archives, options, mm):
  for i, archive in enumerate(archives):
    print('Archive %d data:' % i)
    offset = archive['offset']
    idx = archive['lastIndex']
    timestamp = archive['lastTimestamp'] - idx * archive['secondsPerPoint']
    fmt = archive['format'][-1]
    if fmt in whisper.int_bounds:
      nan = whisper.int_bounds[fmt][2]
    else:
      nan = float("NaN")

    for point in xrange(archive['points']):
      value, = archive['parser'].unpack(
        mm[offset:offset + archive['pointSize']]
      )
      if options.pretty:
        if options.time_format:
          timestr = time.localtime(timestamp)
          timestr = time.strftime(options.time_format, timestr)
        else:
          timestr = time.ctime(timestamp)
      else:
        timestr = str(timestamp)
      if value != nan:
        value = '%10.35g' % value
      else:
        value = ""
      print('%d: %s, %s' % (point, timestr, value))
      offset += archive['pointSize']
      if timestamp == archive['lastTimestamp']:
        timestamp -= archive['retention']
      timestamp += archive['secondsPerPoint']
      if timestamp < 0: break
    print


if not os.path.exists(path):
  raise SystemExit('[ERROR] File "%s" does not exist!' % path)

mm = mmap_file(path)
header = whisper.__readHeader(mm)
dump_header(header)
dump_archives(header['archives'], options, mm)
