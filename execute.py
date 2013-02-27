import os
import sys
import logging

import git

import command_processor

VERSION = '0.0.1'


class SourceRepository(object):
#==============================

  def __init__(self, path=None):
  #-----------------------------
    try:
      self._repo = git.Repo(path)
    except git.exc.InvalidGitRepositoryError:
      raise IOError("Path isn't to a git repository")
    self._base = self._repo.working_dir
    # _repo.git_dir

  def revision(self):
  #------------------
    return self._repo.head.commit.hexsha

  def branch(self):
  #----------------
    return self._repo.active_branch.name

  def path(self):
  #--------------
    return self._base

  def changed_file(self, filename, diff=False):
  #--------------------------------------------
    try:
      gitname = os.path.abspath(filename).split(self._base, 1)[1][1:]
    except IndexError:
      raise KeyError("File outside of controlled directories")
    try:
      cm = self._repo.head.commit
      obj = cm.tree[gitname]
      d = cm.diff(None, paths=[obj.path], create_patch=diff)
      if diff: return d[0].diff if d else ''
      else:    return len(d) != 0
    except KeyError:
      return None if diff else True  ## Need to add file

  def commit(self, files, comment):
  #--------------------------------
    if files:
      idx = self._repo.index
      idx.add(files)
      idx.commit(comment)
      idx.write()

  def tag(self, tag, message):
  #---------------------------
    self._repo.create_tag(tag, message)




if __name__ == '__main__':
#=========================

  if len(sys.argv) < 2:
    sys.exit("Usage: %s [options] COMMAND_FILE [params]" % sys.argv[0])

  cmd = 1     # Position of command file in argument list...
  ## options:  -n      Print commands but without executing them
  ##           ??      Set working directory ??
  ##           ??      Set controlled directory ??
  ##           -m      Comment for committing changes
  ##                   Default is to use timestamp
  ##           -d      Store diffs instead of auto-commit

  ##           -v      Verbose (debug level ?)

  ##                   Specify config flag for commands (other than known ones).
  ##                   List config flags for 'known' commands.

  ## params are expanded in COMMAND_FILE



  logging.getLogger().setLevel(logging.DEBUG)

  commands = list(command_processor.commands(open(sys.argv[cmd], 'r'), sys.argv[cmd:]))


  dryrun = False # True
  autocommit = True
  comment = "Test comment...xxx"  ## With date/time...

  controlled = [ sys.argv[cmd] ]
  for c in commands:
    controlled.extend(c.controlled_files())

  repo = SourceRepository()
  if autocommit:
    changed = [f for f in controlled if repo.changed_file(f)]
    logging.info("Committing: %s", changed)
    if not dryrun: repo.commit(changed, comment)
  else:
    differences = { }
    untracked = [ ]
    for f in controlled:
      diff = repo.changed_file(f, True)
      if diff is None:
        untracked.append(f)
      elif diff:
        differences[f] = diff
    if untracked:
      logging.debug("Untracked: %s", untracked)
      raise KeyError("Untracked files must be added")

  print (repo.path(), repo.branch(), repo.revision())
  ## Provenance includes name (__file__ ??) and version of execute.py

  exitcode = 0
  for c in commands:
    if dryrun:
      sys.stderr.write('\n| '.join([' '.join(cmds) for cmds in c._commands]))
      sys.stderr.write('\n')
    else:
      exitcode = c.run()
  sys.exit(exitcode)
