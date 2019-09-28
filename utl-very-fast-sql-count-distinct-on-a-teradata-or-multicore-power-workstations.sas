Very fast sql count distinct on a teradata or multicore power workstations                                                    
                                                                                                                              
Problem                                                                                                                       
    Count distinct ID for all combinations of  Year and age category  (8 years amd 4 categories = 32 combinations)            
                                                                                                                              
github                                                                                                                        
https://tinyurl.com/y57b23nz                                                                                                  
https://github.com/rogerjdeangelis/utl-very-fast-sql-count-distinct-on-a-teradata-or-multicore-power-workstations             
                                                                                                                              
StackOverflow                                                                                                                 
https://stackoverflow.com/questions/58037095/count-distinct-id-case-when                                                      
                                                                                                                              
Very fast sql count distinct on a teradata or multicore power workstation                                                     
A reasonable compiler will distribute the processing of these count distinct over                                             
many cores.                                                                                                                   
                                                                                                                              
If this is big data ie table > 1tb I would run 8 parallel systasks, one per year..                                            
                                                                                                                              
If data is small I would use 'sql' array below;                                                                               
                                                                                                                              
*_                   _                                                                                                        
(_)_ __  _ __  _   _| |_                                                                                                      
| | '_ \| '_ \| | | | __|                                                                                                     
| | | | | |_) | |_| | |_                                                                                                      
|_|_| |_| .__/ \__,_|\__|                                                                                                     
        |_|                                                                                                                   
;                                                                                                                             
                                                                                                                              
data have;                                                                                                                    
  do id = 1 to 500;                                                                                                           
    do year = 2007 to 2014;                                                                                                   
      if ranuni(123) < 0.75 then continue;                                                                                    
      agecat = ceil(4*ranuni(123));                                                                                           
      output;                                                                                                                 
    end;                                                                                                                      
  end;                                                                                                                        
run;                                                                                                                          
                                                                                                                              
Up to 40 obs WORK.HAVE total obs=963                                                                                          
                                                                                                                              
Obs    ID    YEAR    AGECAT                                                                                                   
                                                                                                                              
  1     1    2007       2                                                                                                     
  2     1    2009       2                                                                                                     
  3     1    2011       2                                                                                                     
  4     1    2014       2                                                                                                     
  5     2    2007       2                                                                                                     
  6     2    2011       1                                                                                                     
  7     2    2012       3                                                                                                     
                                                                                                                              
*            _               _                                                                                                
  ___  _   _| |_ _ __  _   _| |_                                                                                              
 / _ \| | | | __| '_ \| | | | __|                                                                                             
| (_) | |_| | |_| |_) | |_| | |_                                                                                              
 \___/ \__,_|\__| .__/ \__,_|\__|                                                                                             
                |_|                                                                                                           
;                                                                                                                             
                                                                                                                              
WORK.WANT total obs=1                                                                                                         
                                         32                                                                                   
        1         2         3                                                                                                 
                                                                                                                              
      2007                               2007                                                                                 
     agecat=1                           agecat=1                                                                              
                                                                                                                              
      Y20071_   Y20072_   Y20073_  ...  Y20144                                                                                
Obs    IDCNT     IDCNT     IDCNT   ...  IDCNT                                                                                 
                                                                                                                              
 1       35        23        30    ...   33                                                                                   
                                                                                                                              
                    Count                                                                                                     
                  Distinct IDS                                                                                                
                                                                                                                              
 Y2007_1_IDCNT       35                                                                                                       
 Y2007_2_IDCNT       23                                                                                                       
 Y2007_3_IDCNT       30                                                                                                       
 Y2007_4_IDCNT       33                                                                                                       
                                                                                                                              
 Y2008_1_IDCNT       27                                                                                                       
 Y2008_2_IDCNT       30                                                                                                       
 Y2008_3_IDCNT       32                                                                                                       
 Y2008_4_IDCNT       26                                                                                                       
                                                                                                                              
 Y2009_1_IDCNT       26                                                                                                       
 Y2009_2_IDCNT       28                                                                                                       
 Y2009_3_IDCNT       37                                                                                                       
 Y2009_4_IDCNT       30                                                                                                       
 Y2010_1_IDCNT       26                                                                                                       
 Y2010_2_IDCNT       26                                                                                                       
 Y2010_3_IDCNT       37                                                                                                       
 Y2010_4_IDCNT       24                                                                                                       
 Y2011_1_IDCNT       31                                                                                                       
 Y2011_2_IDCNT       32                                                                                                       
 Y2011_3_IDCNT       33                                                                                                       
 Y2011_4_IDCNT       31                                                                                                       
 Y2012_1_IDCNT       27                                                                                                       
 Y2012_2_IDCNT       28                                                                                                       
 Y2012_3_IDCNT       30                                                                                                       
 Y2012_4_IDCNT       38                                                                                                       
 Y2013_1_IDCNT       40                                                                                                       
 Y2013_2_IDCNT       32                                                                                                       
 Y2013_3_IDCNT       31                                                                                                       
 Y2013_4_IDCNT       32                                                                                                       
 Y2014_1_IDCNT       25                                                                                                       
 Y2014_2_IDCNT       24                                                                                                       
 Y2014_3_IDCNT       26                                                                                                       
 Y2014_4_IDCNT       33                                                                                                       
                                                                                                                              
*                                                                                                                             
 _ __  _ __ ___   ___ ___  ___ ___                                                                                            
| '_ \| '__/ _ \ / __/ _ \/ __/ __|                                                                                           
| |_) | | | (_) | (_|  __/\__ \__ \                                                                                           
| .__/|_|  \___/ \___\___||___/___/                                                                                           
|_|                                                                                                                           
;                                                                                                                             
                                                                                                                              
* create year*agecat list;                                                                                                    
%array(agecats,values=%let rc=                                                                                                
        %sysfunc(dosubl('                                                                                                     
         data _null_;                                                                                                         
           length yrage $4096.;                                                                                               
           retain yrage;                                                                                                      
           do yr=2007 to 2014;                                                                                                
              do age=1 to 4;                                                                                                  
                 yrage=catx(" ",yrage,yr!!left(age));                                                                         
              end;                                                                                                            
           end;                                                                                                               
           call symputx("yrage",yrage);                                                                                       
           stop;                                                                                                              
         run;quit;                                                                                                            
         '));                                                                                                                 
         &yrage);                                                                                                             
                                                                                                                              
/*                                                                                                                            
%put &=yrage;                                                                                                                 
YRAGE= 20071 20072 20073 20074                                                                                                
       ....                                                                                                                   
       20141 20142 20143 20144                                                                                                
*/                                                                                                                            
                                                                                                                              
                                                                                                                              
proc sql;                                                                                                                     
  create table want (label="Unique ID count by year") as                                                                      
  select                                                                                                                      
     %do_over(agecats,phrase=                                                                                                 
        count (distinct case when cats(year,agecat)="?"  then id else . end) as y?_idcnt,between=comma                        
     )                                                                                                                        
  from                                                                                                                        
     have                                                                                                                     
;quit;                                                                                                                        
                                                                                                                              
/* if you want the generated code;                                                                                            
                                                                                                                              
     %utlnopts; /*turns everthing off except puts */                                                                          
                                                                                                                              
     %do_over(agecats,phrase=%str(data _null_;                                                                                
        put ",count (distinct case when cats(year,agecat)=?  then id else . end) as y?_idcnt" ;                               
        run;quit;)                                                                                                            
     );                                                                                                                       
                                                                                                                              
,count (distinct case when cats(year,agecat)=20071  then id else . end) as y20071_idcnt                                       
,count (distinct case when cats(year,agecat)=20072  then id else . end) as y20072_idcnt                                       
,count (distinct case when cats(year,agecat)=20073  then id else . end) as y20073_idcnt                                       
,count (distinct case when cats(year,agecat)=20074  then id else . end) as y20074_idcnt                                       
                                                                                                                              
,count (distinct case when cats(year,agecat)=20081  then id else . end) as y20081_idcnt                                       
,count (distinct case when cats(year,agecat)=20082  then id else . end) as y20082_idcnt                                       
,count (distinct case when cats(year,agecat)=20083  then id else . end) as y20083_idcnt                                       
,count (distinct case when cats(year,agecat)=20084  then id else . end) as y20084_idcnt                                       
,count (distinct case when cats(year,agecat)=20091  then id else . end) as y20091_idcnt                                       
,count (distinct case when cats(year,agecat)=20092  then id else . end) as y20092_idcnt                                       
,count (distinct case when cats(year,agecat)=20093  then id else . end) as y20093_idcnt                                       
,count (distinct case when cats(year,agecat)=20094  then id else . end) as y20094_idcnt                                       
,count (distinct case when cats(year,agecat)=20101  then id else . end) as y20101_idcnt                                       
,count (distinct case when cats(year,agecat)=20102  then id else . end) as y20102_idcnt                                       
,count (distinct case when cats(year,agecat)=20103  then id else . end) as y20103_idcnt                                       
,count (distinct case when cats(year,agecat)=20104  then id else . end) as y20104_idcnt                                       
,count (distinct case when cats(year,agecat)=20111  then id else . end) as y20111_idcnt                                       
,count (distinct case when cats(year,agecat)=20112  then id else . end) as y20112_idcnt                                       
,count (distinct case when cats(year,agecat)=20113  then id else . end) as y20113_idcnt                                       
,count (distinct case when cats(year,agecat)=20114  then id else . end) as y20114_idcnt                                       
,count (distinct case when cats(year,agecat)=20121  then id else . end) as y20121_idcnt                                       
,count (distinct case when cats(year,agecat)=20122  then id else . end) as y20122_idcnt                                       
,count (distinct case when cats(year,agecat)=20123  then id else . end) as y20123_idcnt                                       
,count (distinct case when cats(year,agecat)=20124  then id else . end) as y20124_idcnt                                       
,count (distinct case when cats(year,agecat)=20131  then id else . end) as y20131_idcnt                                       
,count (distinct case when cats(year,agecat)=20132  then id else . end) as y20132_idcnt                                       
,count (distinct case when cats(year,agecat)=20133  then id else . end) as y20133_idcnt                                       
,count (distinct case when cats(year,agecat)=20134  then id else . end) as y20134_idcnt                                       
,count (distinct case when cats(year,agecat)=20141  then id else . end) as y20141_idcnt                                       
,count (distinct case when cats(year,agecat)=20142  then id else . end) as y20142_idcnt                                       
,count (distinct case when cats(year,agecat)=20143  then id else . end) as y20143_idcnt                                       
,count (distinct case when cats(year,agecat)=20144  then id else . end) as y20144_idcnt                                       
*/                                                                                                                            
                                                                                                                              
                                                                                                                              
