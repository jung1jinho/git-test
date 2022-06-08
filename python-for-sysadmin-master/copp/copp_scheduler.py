#!/bin/env python
#-*- coding:utf-8 -*-

from syslog import syslog
from cli import cli, clip
import sys
import os

def dirc():
    try:
        cli('mkdir bootflash:scripts/copp-history')
    except:
        pass

def show_set():
    set_mon= 'Y'
    set_mon= raw_input('does yout setting schedule? [Y] ')
    if set_mon == 'y' or set_mon == 'Y' or set_mon=='':
        clip('show scheduler schedule')

def sche_conf(day, hour, minute):
    cli("configure terminal ; no feature scheduler")
    cli("configure terminal ; feature scheduler")
    cli("configure terminal ; \
        scheduler job name copp-log ; \
        python bootflash:/scripts/copp_logging_ver2.py ; \
        exit")
    # print('''configure terminal; \
    #     scheduler job name copp-log; \
    #     python bootflash:/scripts/copp_logging_ver2.py; \
    #     exit''')
    # cli('configure terminal;')
    # cli('scheduler schedule name copp-log;')
    # cli('job name copp-log;')
    # cli('time start now repeat %s:%s:%s'%(day,hour, minute))
    cli("configure terminal ;        \
        scheduler schedule name copp-log ;       \
        job name copp-log ;          \
        time start now repeat %s:%s:%s"%(day,hour, minute))
    # print('''configure terminal; scheduler schedule name copp-log;       \
    # job name copp-log; time start now repeat''')
    # cli("configure terminal ; terminal monitor")

if __name__=='__main__':
    day= sys.argv[1]
    hour= sys.argv[2]
    minute= sys.argv[3]
    print('%s %s %s'%(day,hour, minute))

    dirc()
    sche_conf(day, hour, minute)
    # term_set()
    show_set()
    syslog(2, 'Scheduler write copp-log in bootflash:scripts/copp-history at %sday %stime %sminute intevals'%(day,hour,minute))
