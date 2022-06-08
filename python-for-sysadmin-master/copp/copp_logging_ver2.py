#!/bin/env python
#-*- coding: utf-8 -*-

from copp_check import get_drop_packet
from copp_check import get_policy_class_map
from copp_check import get_copp_status
import time 
from syslog import syslog

def write_syslog(changed_pk_list) :
    global out_file
    class_list = get_policy_class_map()

    now= time.localtime()
    st= '%04d%02d%02d_%02d%02d%02d_'%(now.tm_year, now.tm_mon, now.tm_mday, 
                            now.tm_hour, now.tm_min, now.tm_sec)
    tmp_file= '/bootflash/scripts/copp-history/%scoop.log'%st
    out_file= open(tmp_file, 'w')

    i = 0
    packet_changed = False
    for changed_pk in changed_pk_list :
        if changed_pk != 0 :
            log = "at %s %d packets dropped.\n" % (
                class_list[i], changed_pk)
            packet_changed = True
            out_file.write(log)
        i = i + 1

    if packet_changed :
        write_syslog_policy()
    else:
        log= 'Virtual Box Nexus9000 is not occured packet dropped variation\n'
        out_file.write(log)
    out_file.close()

def write_syslog_policy() :
    copp = get_copp_status()
    if copp == "default value" :
        log = "in addition, the current CoPP is the cisco recommended default."
    else :
        log = "In addition, CoPP policy is not default value. (Present value : %s)" % copp
    out_file.write(log)
out_file= None

if __name__ == "__main__":
    
    # 경과 시간을 문자열(x시 x분 x초)로 변경
    changed_pk = get_drop_packet()
    
    #드랍량을 syslog에 남김
    write_syslog(changed_pk)
