#!/usr/bin/env python
#
# This script is to extract replica learning statistics from log files.
#
# USAGE: python learn_stat.py <log-dir>
#

import re, sys
from os import listdir
from os.path import isfile, join
if len(sys.argv) == 1:
  print "USAGE:",sys.argv[0],"<log-dir>"
  sys.exit(1)
dir = sys.argv[1]
file_ids = sorted([int(f[4:][0:-4]) for f in listdir(dir) if isfile(join(dir, f)) and f.startswith('log.') and f.endswith('.txt')])
p_id = re.compile(' ([0-9.]+)@[0-9.:]+: .*\[([0-9]+)\]: ')
p_decree = re.compile('app_committed_decree = ([0-9]+)')
p_learner = re.compile(' [0-9.]+@([0-9.:]+): init_learn')
p_learnee = re.compile('learnee = ([0-9.:]+)')
p_duration = re.compile('learn_duration = ([0-9]+)')
p_meta_size = re.compile('on_learn_reply.*learned_meta_size = ([0-9]+), learned_file_count = ([0-9]+)')
learn_map = {}
for fid in file_ids:
  fname = 'log.'+str(fid)+'.txt'
  fpath = join(dir,fname)
  with open(fpath) as f:
    for line in f:
      if 'replica.learn:' not in line:
        continue
      # id
      m = p_id.search(line)
      if not m:
        continue
      gpid = m.group(1)
      signature = m.group(2)
      id = gpid+'#'+m.group(2)
      if id not in learn_map:
        learn = {'gpid':gpid,'signature':signature,'log_file':fname,'learn_round':0,'duration':0,'meta_size':0,'file_count':0,'completed':False}
        learn_map[id] = learn
      else:
        learn = learn_map[id]
      # learn_round & start_decree
      if 'init_learn' in line:
        learn['learn_round'] += 1
        if 'start_decree' not in learn:
          m = p_decree.search(line)
          if m:
            start_decree = int(m.group(1))
            learn['start_decree'] = start_decree
      # learner
      if 'learner' not in learn:
        m = p_learner.search(line)
        if m:
          learner = m.group(1)
          learn['learner'] = learner
      # learnee
      if 'learnee' not in learn:
        m = p_learnee.search(line)
        if m:
          learnee = m.group(1)
          learn['learnee'] = learnee
      # duration
      m = p_duration.search(line)
      if m:
        duration = int(m.group(1))
        if duration > learn['duration']:
          learn['duration'] = duration
      # meta_size
      m = p_meta_size.search(line)
      if m:
        meta_size = int(m.group(1))
        file_count = int(m.group(2))
        learn['meta_size'] += meta_size
        learn['file_count'] += file_count
      # completed
      if not learn['completed'] and 'notify_learn_completion' in line:
        learn['completed'] = True
        if 'end_decree' not in learn:
          m = p_decree.search(line)
          if m:
            end_decree = int(m.group(1))
            learn['end_decree'] = end_decree
        if 'start_decree' in learn and 'end_decree' in learn:
          learn['increased_decree'] = learn['end_decree'] - learn['start_decree'] + 1
        print learn
print
print '=========================================================='
print
for id, learn in learn_map.items():
  if not learn['completed']:
    print learn

