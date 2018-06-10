# Insight Challenge

## Initialization

* Directories were created according to Insight's repo directory structure
* Data gathered from SEC weblogs
- download data - `wget http://www.sec.gov/dera/data/Public-EDGAR-log-file-data/2017/Qtr1/log20170101.zip` and `wget http://www.sec.gov/dera/data/Public-EDGAR-log-file-data/2017/Qtr1/log20170102.zip`
- unzipped and catted together into log.csv
- Log file is 3.7GB in size
- Format is a comma-delimited file, where each line is a request. Parameters of interest are: ip, and the date and time

## Methods - Detailed description of stepsf in sessionization.py

Specifics of the code are available as comments in the main program - sessionization.py. The objective is to count the number of requests for a user, defined as a session.

The code begins by accepting required parameters: input, period, and output. Input is the path to the log file. Period is the path to inactivity_period.txt, which determines how long a session is. Output is the path to the number of requests for a session.

For extensibility, an object termed `logs` in the program is instantiated for the log file. This allows future work to allow input to be a directory, then adding a loop to generate an object for each log file in the directory.

The main function of `logs` is called.

The log file is formatted so that the second line contains the earliest date in the file. Collecting this and formatting it as a datetime is done. Then the log file is rewound back so the first request can be manipulated in the same way as other requests.

Each request is a line in the log file. For each request, the parameters of interest are collected. Due to format of the file, any ip that is equal to "ip" indicates a header row and should be skipped.

The time of the request is formatted as a datetime object by concatting the date and time of the request.

Python dictionaries allow O(1) lookup times. It was determined this structure allowed for fast implementation and fast lookups. A dictionary termed `self.session_dict` holds the ip of the request, the start and end time of the request, a count for the number of requests, and an index. An index is necessary to preserve order the request came in to the order in the log file. The dictionary only contains in-progress sessions. If the dictionary does not contain a requests ip, that means it's a new session and is added to the dictionary. If a request's ip is found in the dictionary, then a session is still in progress and its count is incremented and its end time updated.

As inactivity period is measured in seconds, it made sense to reduce the number of checks to once a second. This is done by knowing when a requests time is different than the previous request. Additionally, by checking for difference, the code can handle the unlikely event of a longer interval between requests. At this time, the function `to_output` is called followed by updating the previous time.

`to_output` is written to check if a session's inactivity period has elapsed. It has optional parameters that allows for a check to occur both while reading over the file, and at the end of the file. The only way to know if a session has ended is to loop over all ongoing sessions and check their times. If the difference between the current time and a session's end time is greater than the inactivity period, or if it's the end of the log file, then the session has ended. If it's the end of the session, then this is written as the end time for the session. Next, the session period is calculated as the difference between end time and start time. The count was already calculated in the main loop. Finally, all completed sessions are added to an array for deletion (python discourages deletion from a dictionary that is being looped over).

As a requirement is to preserve order, the deletion array is sorted by start time and index (where index is line number in the log file). This allows requests that have the same start time to be written to output in the same order they came in.

Once the array has been sorted, the output file is written in the desired format. The completed sessions are deleted from the dictionary.

Once everything is complete, the input and output files are closed.

## Results and future directions

Using python dictionaries is a scalable way to handle large amounts of sessions with low overhead. By pruning the dictionary when a session expires, and reducing the number of checks to once a second, performance is gained. This method has trouble scaling when the inactivity period becomes large, as the memory required to hold the dictionary grows.

On the author's PC, with an inactivity period of two seconds and 3.7GB of log data, took about 800 seconds ~ 13 minutes and the dictionary contained at most 207 active sessions. This is reasonable but doesn't scale very well. For example a month of log data would take about three hours. As order reading from the file is important, it is difficult to concieve of a distributed manner to solve the prompt. A simple way of scaling is to keep log files per day and distribute to multiple computers at once. This will reduce the total time to a single day, about 6.5 minutes. However this fails to account for sessions that overlap days. If order is relaxed to only time, then it's feasible a map reduce could be used. An initial attempt would start with mapping a key-value where the key is a tuple of user-time and a value of one. Then a reduce to sum the number of requests. As common implementations of reduce sort the output, this can then be followed by iterating over the output in a similar manner to the dictionary method. Performance is gained by reducing the size of the data iterating over. However this is not significantly different from the current method. 

