# 6.824-ads-labs    
   
the 6.824 labs  
This is my answer of 6.824  
As a greenhand of the ads-lab, I also read the code from     
https://github.com/WenbinZhu/MIT-6.824-labs      
https://github.com/xingdl2007/6.824-2017    
The code from Wenbinzhu,xingdl2007 is clear and easy to understand.    
I added some comments in my version so that it would be easy to understand how the implementation works.    

Course website: https://pdos.csail.mit.edu/6.824/schedule.html  

There are several branch in this repository. Why did i create so many branch?   
I think it will be convenient for you to check the answer of a specific steps in a lab.  
  
for example:  
    lab1_part1: only part1 code  
    lab2_part2: part1 + part2  
    lab2_part3: part2 + part3  
      
It's clear that you can get the pure step1 answer in branch-step1 without any other succeeding step code in it.  

Btw, the master branch is the original clean 6.824 code from the MIT-6.824 class  
And the dev branch is a branch maintaining the latest progress which follows the list below.  

- [x] Lab 1: MapReduce

- [x] Lab 2: Raft Consensus Algorithm
- [x] ----Lab 2-part2A
- [x] ----Lab 2-part2B
- [x] ----Lab 2-part2C

- [x] Lab 3: Fault-tolerant Key/Value Service
- [x] ----Lab 3-part3A
- [x] ----Lab 3-part3B

- [ ] Lab 4: Sharded Key/Value Service
- [x] ----Lab 4-part4A
- [ ] ----Lab 4-part4B    
   --------Lab4B is quite difficult for me, and here are some logical diagrams refered to https://github.com/xingdl2007/6.824-2017 implementation of shardkv.        
       
How the shard\group\server combine together:     
![combination](https://github.com/bugzyz/6.824-ads-labs/raw/dev/readmePic/IMG_5343.jpg)     
        
Configure -> Migration:     
![Configure -> Migration](https://github.com/bugzyz/6.824-ads-labs/raw/dev/readmePic/IMG_5344.jpg)      
        
shardkv applyDaemon():     
![shardkv applyDaemon()](https://github.com/bugzyz/6.824-ads-labs/raw/dev/readmePic/IMG_5342.jpg)  
