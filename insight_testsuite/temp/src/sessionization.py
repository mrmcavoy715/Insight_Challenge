'''
progam: sessionization.py
author: Matthew McAvoy

Use: Solution to Insight's data engineering challenge
'''

import argparse
import datetime
from timeit import timeit


class userLogs(object):

    def __init__(self, parser):
        '''
        userlogs is a class that loads a log file, operates on it to 
        count the number of requests for a user per session, and 
        writes it to output

        Definitions
          self.args: accepts inputs from the command line

          self.data: opens the input log file

          self.output: opens writable output file

          self.inactivity_period: value determining inactivity period

          self.user_dict: dict of dict. First key is user, 
            second key are various parameters for that user

          self.del_user: array for users that are ready to be deleted
            from self.user_dict

          self.max_users: largest number of  users in self.user_dict
            at one point during program run
        
        '''

        self.args = parser.parse_args()
        self.data = open(self.args.input)
        self.output = open(self.args.output, 'w')
        self.inactivity_period_file = open(self.args.period)
        self.inactivity_period = int(self.inactivity_period_file.readline())

        self.session_dict = {}
        self.del_session = []
        self.max_sessions = 0

    def to_output(self, current_time, last_line = False):
        '''
        Check all users whether their session has ended. If it has, add 
        that user's dict as one to write to output and them remove from 
        the operating dict.
        '''

        for session in self.session_dict:

            # time_delta is time difference between session's last request
            # and the current time
            time_delta = current_time - self.session_dict[session]['end_time']
            time_delta = int(time_delta.total_seconds())
            
            # if the session has passed the inactivity period, or it's the
            # end of the file, update the session's period of activity
            # (time_delta_session), and add to array.
            if time_delta > self.inactivity_period or last_line == True:
                if time_delta > self.inactivity_period and last_line == True:
                    self.session_dict[session]['end_time'] = current_time
                time_delta_session = self.session_dict[session]['end_time'] - self.session_dict[session]['start_time']
                time_delta_session = int(time_delta_session.total_seconds()) + 1
                #print("time delta for session", time_delta, time_delta_session, session)
                self.session_dict[session]['time_delta'] = time_delta_session
                self.del_session.append(self.session_dict[session])
                    
        # sort array based on session's start time and order encountered
        self.del_session = sorted(self.del_session, key=lambda key: (key['start_time'], key['index']))
        #print("current time", current_time, "will delete", len(self.del_session), "number of sessions")

        # for each ending session, write to output and delete from dict
        for d in self.del_session:
            session = d['ip']
            #print("deleting session", session, self.session_dict[session]['start_time'], self.session_dict[session]['end_time'], self.session_dict[session]['count'], self.session_dict[session]['index'])
            output_line = session + ',' + self.session_dict[session]['start_time'].strftime("%Y-%m-%d %H:%M:%S") + ',' + self.session_dict[session]['end_time'].strftime("%Y-%m-%d %H:%M:%S") + ',' + str(self.session_dict[session]['time_delta']) + ',' + str(self.session_dict[session]['count']) + '\n'
            #print("ending session", output_line)
            self.output.write(output_line)
            del self.session_dict[session]

        # now that all ending session's are done, remove from list of
        # sessions to delete
        self.del_session = []
        
    
    def main(self):
        '''
        Main program that reads log file, gathers important parameters
        from each request.
        '''
        
        # open the log file
        with open(self.args.input) as f:

            # read the first line to get the first date, then point
            # the file back to the start so the first request
            # isn't missed
            first_line_pointer = f.tell()
            first_line = f.readline()
            second_line = f.readline()
            previous_time = datetime.datetime.strptime(' '.join(second_line.split(',')[1:3]), "%Y-%m-%d %H:%M:%S")
            f.seek(first_line_pointer)
            print("first log time", previous_time)

            # iterate over lines in the log file
            for i, line in enumerate(f):

                # parameters
                # ip,date,time,zone,cik,accession,extention,code,size,idx,norefer,noagent,find,crawler,browser
                # split each log to get the ip and date-time
                line_split = line.split(',')
                ip = line_split[0]
                date = line_split[1]
                time = line_split[2]

                # skip if a header row
                if ip == 'ip':
                    continue

                # convert date and time to datetime dtype
                current_time = datetime.datetime.strptime(date + ' ' + time, "%Y-%m-%d %H:%M:%S")

                # add a new user or returning user
                if ip not in self.session_dict:
                    self.session_dict[ip] = { 'ip': ip, 'start_time': current_time, 'end_time': current_time, 'count': 1, 'index': i }
                # update sessions time and count if session is active
                else:
                    self.session_dict[ip]['end_time'] = current_time
                    self.session_dict[ip]['count'] += 1

                # if a second has passed, call to_output to check elapsed time
                if (current_time - previous_time).total_seconds() > 0:
                    
                    # self check to know max number of sessions
                    # in self.session_dict
                    session_count = len(self.session_dict)
                    
                    if session_count > self.max_sessions:
                        self.max_sessions = session_count
                    
                    self.to_output(current_time)

                    # update previous time
                    previous_time = current_time
        
        # Once log file has reached the end, write all
        # remaining active sessions
        self.to_output(previous_time, last_line=True)

        print("last log time", previous_time)        
        print("largest number of items in self.session_dict", self.max_sessions)
        
                    
    def close_file(self):

        self.data.close()
        self.inactivity_period_file.close()
        self.output.close()


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="""
    Help page for sessionization.py program

    Requires three inputs in this order
      * an input log file
      * a file that defines the period
      * an output file
    """, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("input", type=str, nargs='?',  help="Path to input file")
    parser.add_argument("period", type=str, nargs='?',  help="Path to inactivity_period file")
    parser.add_argument("output", type=str, nargs='?', help="Path to output file")

    # instantiate logs object
    logs = userLogs(parser)

    # call main function
    print("Starting sessionization.py program to calculate number of requests by a user during a session")
    print(timeit(lambda: logs.main(), number = 1))

    # close files before exiting
    logs.close_file()


