'''
Init sessionization.py

Takes data from input/log.csv, streams and calculates user sessions
'''

import argparse
import datetime
from timeit import timeit


class logIterable(object):

    def __init__(self, parser):

        self.args = parser.parse_args()
        #print("input_path", self.args.input)
        #print("output path", self.args.output)
        # parameters
        # ip,date,time,zone,cik,accession,extention,code,size,idx,norefer,noagent,find,crawler,browser
        self.data = open(self.args.input)
        self.output = open(self.args.output, 'w')
        self.inactivity_period = int(open(self.args.period).readline())
        #print("inactivity period", self.inactivity_period)

        self.user_dict = {}
        self.del_user = []
        self.max_users = 0


    def __iter__(self):

        #line = self.data.readline()
        #yield print(line.split(',')[0:2])
        pass

    def to_output(self, current_time, last_line = False):

        for user in self.user_dict:

            #time_delta = self.user_dict[user]['end_time'] - self.user_dict[user]['start_time']
            time_delta = current_time - self.user_dict[user]['end_time']
            time_delta = int(time_delta.total_seconds())
            

            if time_delta > self.inactivity_period or last_line == True:
                if time_delta > self.inactivity_period and last_line == True:
                    self.user_dict[user]['end_time'] = current_time
                time_delta_user = self.user_dict[user]['end_time'] - self.user_dict[user]['start_time']
                time_delta_user = int(time_delta_user.total_seconds()) + 1
                #print("time delta for user", time_delta, time_delta_user, user)
                self.user_dict[user]['time_delta'] = time_delta_user
                self.del_user.append(self.user_dict[user])
                    
                    
        self.del_user = sorted(self.del_user, key=lambda key: (key['start_time'], key['index']))
        #print("current time", current_time, "will delete", len(self.del_user), "number of users")

        for d in self.del_user:
            user = d['ip']
            #print("deleting user", user, self.user_dict[user]['start_time'], self.user_dict[user]['end_time'], self.user_dict[user]['count'], self.user_dict[user]['index'])
            output_line = user + ',' + self.user_dict[user]['start_time'].strftime("%Y-%m-%d %H:%M:%S") + ',' + self.user_dict[user]['end_time'].strftime("%Y-%m-%d %H:%M:%S") + ',' + str(self.user_dict[user]['time_delta']) + ',' + str(self.user_dict[user]['count']) + '\n'
            #print("ending session", output_line)
            self.output.write(output_line)
            del self.user_dict[user]

        self.del_user = []
        
    
    def open_file(self):

        # open the log file
        with open(self.args.input) as f:

            # read the first line to get the first date
            first_line_pointer = f.tell()
            first_line = f.readline()
            second_line = f.readline()
            previous_time = datetime.datetime.strptime(' '.join(second_line.split(',')[1:3]), "%Y-%m-%d %H:%M:%S")
            
            print("start_time", previous_time)
            f.seek(first_line_pointer)

            # iterate over logs
            for i, line in enumerate(f):

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
                if ip not in self.user_dict:
                    self.user_dict[ip] = { 'ip': ip, 'start_time': current_time, 'end_time': current_time, 'count': 1, 'index': i }
                # update users time and count if user is active
                else:
                    self.user_dict[ip]['end_time'] = current_time
                    self.user_dict[ip]['count'] += 1

                # if a second has passed, call to_output to check elapsed time
                # for all users
                if (current_time - previous_time).total_seconds() > 0:
                    
                    # self check number of items in dict at any certain time
                    user_count = len(self.user_dict)
                    
                    if user_count > self.max_users:
                        self.max_users = user_count
                    
                    self.to_output(current_time)
                    
                    previous_time = current_time
        
        # Write remaining users at end
        self.to_output(previous_time, last_line=True)

        print("last time", previous_time)        
        print("largest number of items in self.user_dict", self.max_users)
        
                    
    def close_file(self):

        self.data.close()
        self.output.close()


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="""
    """, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("input", type=str, nargs='?',  help="Path to input data")
    parser.add_argument("period", type=str, nargs='?',  help="Path to input data")
    parser.add_argument("output", type=str, nargs='?', help="Path to output data")
    
    start = logIterable(parser)
    print(timeit(lambda: start.open_file(), number = 1))
    start.close_file()


    '''
    Three map reduces?
    First: map ip-second
    Second: ip
    Third: sort
    '''
