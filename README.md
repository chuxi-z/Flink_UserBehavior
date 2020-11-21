## UserBehavior -- Flink

### HotItems


    

### LoginDetect



### NetworkStatistics
#### Data source 
- apache server log file

#### Requirements
- Statistics of real-time access traffic from web server logs.  
- Count the number of IP visits per minute, take the 5 most visited addresses, and update every 5 seconds.

#### Solution 
- Convert the time in the apache server log into timestamp.  
- Construct a sliding window with a window length of 1 minute and a sliding distance of 5 seconds.  

