## Design goals

### 1. execute rules on one or multiple streams

### 2. dynamically inject new rules on existing streams

### 3. reuse streams as much as possible

## Primivite operations

### 1. rules on single stream keyed by some fields
 avg(cpu) > 0.8 [1m] group by host 

 sum(failed_requests) > 60 [1m] group by host
 
 avg(failure_ratio) > 0.1 [1m] group by host
 
### 2. rules on multiple streams joined by some fields
 
 
 
 
