'''
Created on Apr 3, 2018
Varsha Bakshani
Insight Data Engineering Application New York 
'''

import csv
import dateutil as du
import dateutil.parser as p
import datetime
import sys

def process(infile, outfile, gap_length):
    '''
    this is the main processing loop that reads the records line by line, 
    sorts the records in memory, and then processes them in order
    '''
    recs = list()
    rdr = csv.reader(infile)
    next(rdr,None)                  # skip the header
    # read into a vector
    count = 0
    print("    Starting read at : {0}".format(datetime.datetime.now()))

    for r in rdr:
        recs.append([r[0], p.parse(r[1]+"T"+r[2])])
        count += 1
        if (count % 1000000) == 0:
            print("    {0:,} records processed".format(count))
    print("    Ending read at  :  {0}".format(datetime.datetime.now()))
    print("    {0:,} total records processed".format(count))

    # print(recs[:1])
    print("    Starting sort at : {0}".format(datetime.datetime.now()))
    recs.sort(key=lambda s: (s[0], s[1]))
    print("    Ending sort at   : {0}".format(datetime.datetime.now()))
    
    # recs is now sorted by ip, date, time
    rec         = recs[0]
    session_ip  = rec[0]
    session_st  = rec[1]
    session_et  = rec[1]
    req_count   = 1
    for rec in recs:
        if (session_ip != rec[0]
            or ((rec[1] - session_et).total_seconds()) > gap_length):
            out_rec = "{0},{1},{2},{3},{4}\n".format(
                session_ip,         # ip
                session_st.strftime("%Y-%m-%d %H:%M:%S"),
                session_et.strftime("%Y-%m-%d %H:%M:%S"),
                round((session_et-session_st).total_seconds()+1),
                req_count)
            print(out_rec,end="")
            outfile.write(out_rec)
            session_ip = rec[0]
            session_st = rec[1]
            session_et = rec[1]
            req_count = 1
        else:
            req_count += 1
            session_et = rec[1]
            
    # when you've run out of records, output the final 
    out_rec = "{0},{1},{2},{3},{4}\n".format(
                session_ip,         # ip
                session_st.strftime("%Y-%m-%d %H:%M:%S"),
                session_et.strftime("%Y-%m-%d %H:%M:%S"),
                round((session_et-session_st).total_seconds()+1),
                req_count)
    print(out_rec,end="")
    outfile.write(out_rec)
            
    return count

def main(ifname, ofname):
    with open("input/inactivity_period.txt") as f:
        gap_length = int(f.read())
    print("inactivity period: {0}".format(gap_length))    
    with open(ifname) as infile:
        with open(ofname,"w") as outfile:
            rec_count = process(infile, outfile, gap_length)
        print("Processed {0:,} records".format(rec_count))


if __name__ == '__main__':
    print("Starting at : {0}".format(datetime.datetime.now()))
    main(sys.argv[1], sys.argv[2])
    print("Ending at :   {0}".format(datetime.datetime.now()))
