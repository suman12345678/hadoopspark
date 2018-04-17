#problem
#=========
#movies.dat MovieID – Title – Genres 
#ratings.dat UserID – MovieID – Rating – Timestamp 
#users.dat UserID – Gender – Age – Occupation – ZipCode

#1.   Top ten most viewed movies with their movies Name (Ascending or Descending order)? 
#2.   Top twenty rated movies (Condition: The movie should be rated/viewed by at least 40 users) 
#3.    We wish to know how have the genres ranked by Average Rating, for each profession and age group. The age groups to be considered are: 18-35, #36-50 and 50+. 



import sys
#rm -rf /home/cloudera/Desktop/ml-1m/1
#touch /home/cloudera/Desktop/ml-1m/1
#spark-submit --master local /home/cloudera/Desktop/ml-1m/Ass1.py /home/cloudera/Desktop/ml-1m/ratings.dat /home/cloudera/Desktop/ml-1m/movies.dat /home/cloudera/Desktop/ml-1m/users.dat /home/cloudera/Desktop/ml-1m/1

from pyspark import SparkContext,SparkConf
from pyspark import SQLContext,Row


import os

if __name__ == "__main__":

  
  conf=SparkConf().setAppName("Most watched movie")
  sc=SparkContext(conf=conf)
  
  ratingsRDD=sc.textFile(sys.argv[1]).map(lambda x:x.split("::"))
  sqc=SQLContext(sc)  
  ratingssql=ratingsRDD.map(lambda x: Row(userid=x[0],movieid=x[1],rating=x[2]))
  schema=sqc.inferSchema(ratingssql)
  schema.registerTempTable("ratings")
  #c=sqcratings.sql("select movieid,COUNT(*) as count1 from ratings GROUP BY movieid order by count1 desc limit 10 ")
  
  moviesRDD=sc.textFile(sys.argv[2]).map(lambda x:x.split("::"))
  #sqcmovies=SQLContext(sc)  
  moviessql=moviesRDD.map(lambda x: Row(movieid=x[0],title=x[1],genre=x[2]))
  schema=sqc.inferSchema(moviessql)
  schema.registerTempTable("movies")
  #c=sqcmovies.sql("select distinct(userid) from movies limit 4")

  usersRDD=sc.textFile(sys.argv[3]).map(lambda x:x.split("::"))
  #sqcusers=SQLContext(sc)  
  userssql=usersRDD.map(lambda x: Row(userid=x[0],age=x[2],occupation=x[3]))
  schema=sqc.inferSchema(userssql)
  schema.registerTempTable("users")
  #c=sqcusers.sql("select distinct(userid) from movies limit 4")
  # assignment 1 
  c=sqc.sql("select a.movieid,b.title,count(rating) as count1 from ratings a left join movies b on a.movieid = b.movieid group by a.movieid,b.title order by count1 desc limit 10")
  result1=c.map(lambda x: (x[1],x[2]))
  #write to a file
  f=open(sys.argv[4],"w")
  for i in result1.collect():
    f.write(i[0]+str(i[1])+'\n')


  # assignment 2
  c=sqc.sql("select a.movieid,b.title,avg(rating) as count1,count(rating) as count2 from ratings a left join movies b on a.movieid = b.movieid group by a.movieid,b.title having count2>=40 order by count1 desc limit 20")
  result1=c.map(lambda x: (x[1],x[2]))
  #write to a file
  f=open(sys.argv[5],"w")
  for i in result1.collect():
    f.write(i[0]+str(i[1])+'\n')

  # assignment 3
  c=sqc.sql("select case when c.occupation='0' then 'other/ or not specified' when c.occupation='1' then 'academic/educator' when c.occupation='2' then 'artist' when c.occupation='3' then 'clerical/admin' when c.occupation='4' then 'college/grad student' when c.occupation='5' then 'customer service' when c.occupation='6' then 'doctor/health care' when c.occupation='7' then 'executive/managerial' when c.occupation='8' then 'farmer' when c.occupation='9' then 'homemaker' when c.occupation='10' then 'K-12 student' when c.occupation='11' then 'lawyer' when c.occupation='12' then 'programmer' when c.occupation='13' then 'retired' when c.occupation='14' then 'sales/marketing' when c.occupation='15' then 'scientist' when c.occupation='16' then 'self-employed' when c.occupation='17' then 'technician/engineer' when c.occupation='18' then 'tradesman/craftsman' when c.occupation='19' then 'unemployed' when c.occupation='20' then 'writer' else 'not any occupation' end as occupation1 , case when c.age = '1' then 'Under 18' when c.age = '18' then '18-35' when c.age = '25' then '18-35' when c.age = '35' then '36-50' when c.age = '45' then '36-50' when c.age = '50' then '50+' when c.age = '56' then  '50+' else 'NOT DEFINED' end as age1, b.genre, avg(a.rating) as avg1 from users c left join ratings a on c.userid=a.userid left join movies b on a.movieid=b.movieid group by c.occupation,c.age,b.genre order by c.occupation,c.age,avg1 desc")

  # now get top 20 records
  def getfive(accu,x):
    prev=""
    while(prev!=x and len(accu.split("$$"))<6):
      accu=accu+"$$"+x
      prev=x
    return accu
  

  finalRDD=sortavg1RDD.reduceByKey(getfive)  # [(u'writer:::18-35', 'Sci-Fi$$Drama|Thriller|War$$Drama|Thriller|War$$Drama|Thriller|War$$Drama|Thriller|War')
  #finalRDD.saveAsTextFile(sys.argv[4])
  # write without u
  f=open(sys.argv[4],"w")

  final=finalRDD.map(lambda x: (x[0],x[1]))
  for i in final.collect():  #now its python object and not RDD
    f.write(i[0]+"##"+i[1]+'\n') 
