"""
Command files
=============

Essentially bash commands without pipe, stdio redirection and line continuation, these
being indicated by the first character of each line:

Valid values for the first character are ' ', '<', '|', '>', '-' and '#'.

  *  ' ' means the remainder of the line is a command (and parameters).
  *  '<' is followed by a file name and means to output the file to stdout
     (and is equivalent to the line ' cat file_name').
  *  '|' means the remainder of the line is a command (and parameters), whose
     standard input is the output from the preceding command.
  *  '>' is followed by a file name and means to save the preceding command's
     standard output in the file. If a second '>' precedes the filename then
     output is appended to the file.
  *  '-' indicates a continuation of the previous line.
  *  '#' means the line contains a comment and is ignored.
  *  Blank lines are ignored.


An example (extract and show data in a CPAP file):::

    echo "ibase=16; `cat input_flow.data | tr \" \" \"\n\"`"
  | bc -l
  | awk '{
  -   if ($0 >= 32768) print i++, $0-65536;
  -      else print i++, $0;
  -      }'
  | pertecs -c plot -ctrls -rtc -rate 1000000 -ineof


Another example (compile and run a program):::

  # Compile and run a program
    cc -c f1.c
    cc -c f2.c
    cc -o test f1.o f2.o
    ./test


Syntax
------

  * ' ' or '<' indicates the start of a command pipeline.
  * ' ' or '>' indicates the end of a command pipeline.
  * '|' cannot follow a '>'.
  * '-' cannot follow '<' or '>'.
  * '<' cannot follow '<'.
  * '>' cannot follow '<'.

"""

import sys
import shlex
import signal
import subprocess


CONFIG_OPTIONS = { 'pertecs': ('-c', '\\.*') }


class Command(object):
#=====================

  def __init__(self, input, cmds, output):
  #---------------------------------------
    self._input = input
    self._commands = cmds
    if output is not None and output.startswith('>'):
      self._output = output[1:].strip()
      self._outputmode = 'a'
    else:
      self._output = output
      self._outputmode = 'w'
    self._process = None

  def controlled_files(self):
  #--------------------------
    controlled = [ ] if self._input is None else [self._input]
    for cmd in self._commands:
      try:
        option = CONFIG_OPTIONS.get(cmd[0])
        controlled.append(cmd[cmd.index(option[0]) + 1] + option[1])
      except (TypeError, ValueError, IndexError):
        pass
    return controlled

  def interrupt(self, signum, frame):
  #----------------------------------
    if self._process is not None:
      self._process.send_signal(signum)

  def run(self):
  #-------------
    signal.signal(signal.SIGINT, self.interrupt)
    stdin = (sys.stdin if self._input in [None, ''] else
             open(self._input, 'r'))
    stdout = subprocess.PIPE
    lastcmd = (len(self._commands) - 1)
    for n, cmd in enumerate(self._commands):
      if n == lastcmd:
        stdout = (sys.stdout if self._output in [None, ''] else
                  open(self._output, self._outputmode))
      self._process = subprocess.Popen(cmd, stdin=stdin, stdout=stdout)
      # Could save process ids -- process.pid
      if n > 0: stdin.close()
      stdin = self._process.stdout
    return self._process.wait()


def commands(script, params):
#============================

  def clean(line):
  #---------------
    return l[1:].strip()

  def expand(word):
  #----------------
    i = 0
    result = [ ]
    while True:
      d = word[i:].find('$')
      if d < 0:
        result.append(word[i:])
        break
      d += i       # Index into word, not sub-string
      i = d + 1
      while word[i:i+1].isdigit():
        i += 1
      if i == (d+1):        # '$' without following parameter number
        result.append(word[d:i])
      else:
        p = int(word[d+1:i])
        if p < len(params): result.append(params[p])
        else:               result.append('')
    return ''.join(result)

  def expanded(cmds):
  #------------------
    return [[expand(c) for c in shlex.split(cmd)] for cmd in cmds]

  cmds = [ ]
  input = None
  output = None
  for l in script:
    l = l.rstrip()  # Removes '\n'
    if l == '' or l.strip()[0] == '#':
      continue
    if l[0] in ['<', ' ']:
      if cmds:
        yield Command(input, expanded(cmds), output)
        output = None
        input = None
        cmds = [ ]
      if l[0] == '<':
        if input is not None:
          raise ValueError("No command to send input to")
        input = expand(shlex.split(clean(l))[0])
      else:
        if input is not None:
          raise ValueError("Input needs a pipe")
        cmds.append(clean(l))
    elif l[0] == '-':
      if not cmds:
        raise ValueError("No command to continue")
      cmds[-1] += ' ' + clean(l)
    elif l[0] == '|':
      if input is None and not cmds:
        raise ValueError("No preceding command")
      cmds.append(clean(l))
    elif l[0] == '>':
      if not cmds:
        raise ValueError("No preceding command")
      output = expand(shlex.split(clean(l))[0])
      yield Command(input, expanded(cmds), output)
      output = None
      input = None
      cmds = [ ]
  if cmds: yield Command(input, expanded(cmds), output)


if __name__ == '__main__':
#=========================

  if len(sys.argv) < 2:
    sys.exit("Usage: %s [options] COMMAND_FILE [params]" % sys.argv[0])

  cmd = 1     # Position of command file in argument list...
  ## options:  -n      Print commands but without executing them
  ##           ??      Set working directory ??
  ##           ??      Set controlled directory ??

  ## params are expanded in COMMAND_FILE

  exitcode = 0

  for c in commands(open(sys.argv[cmd], 'r'), sys.argv[cmd:]):
    print 'C:', c.controlled_files()
    exitcode = c.run()

  sys.exit(exitcode)
