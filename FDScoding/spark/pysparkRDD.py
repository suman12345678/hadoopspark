#problem
#=========
#movies.dat MovieID – Title – Genres 
#ratings.dat UserID – MovieID – Rating – Timestamp 
#users.dat UserID – Gender – Age – Occupation – ZipCode

#1.   Top ten most viewed movies with their movies Name (Ascending or Descending order)? 
#2.   Top twenty rated movies (Condition: The movie should be rated/viewed by at least 40 users) 
#3.    We wish to know how have the genres ranked by Average Rating, for each profession and age group. The age groups to be considered are: 18-35, 36-50 and 50+. 


#ass1
#====

import sys
#rm -rf /home/cloudera/Desktop/ml-1m/1
#touch /home/cloudera/Desktop/ml-1m/1
#spark-submit --master local /home/cloudera/Desktop/ml-1m/Ass1.py /home/cloudera/Desktop/ml-1m/ratings.dat /home/cloudera/Desktop/ml-1m/movies.dat /home/cloudera/Desktop/ml-1m/1

from pyspark import SparkContext,SparkConf
import os

if __name__ == "__main__":

  conf=SparkConf().setAppName("Most watched movie")
  sc=SparkContext(conf=conf)
  #input file => 1::1193::5::978300760
  ratingsRDD=sc.textFile(sys.argv[1]).map(lambda x:x.split("::"))
  #output => [u'1', u'1193', u'5', u'978300760'], [u'1', u'661', u'3', u'978302109'],
  ratings1RDD=ratingsRDD.map(lambda x: (x[1]+":::",1))
  #output => [(u'1193:::', 1), (u'661:::', 1),
  ratings2RDD=ratings1RDD.reduceByKey(lambda x,y : x+y)
  #output => [(u'2219:::', 10), (u'3260:::', 480)
  #input file => 1::Toy Story (1995)::Animation|Children's|Comedy
  moviesRDD=sc.textFile(sys.argv[2]).map(lambda x:x.split("::"))
  #output => [[u'1', u'Toy Story (1995)', u"Animation|Children's|Comedy"], 
  movies1RDD=moviesRDD.map(lambda x:(x[0]+":::",x[1]))
  #output ==> [(u'1:::', u'Toy Story (1995)'), 
  finalRDD=movies1RDD.join(ratings2RDD)  # (movie id ,(title,viewed)) ==> [(u'2027:::', (u'Mafia! (1998)', 129))
  final1RDD=finalRDD.map(lambda x: (x[1][1],str("##")+x[0]+x[1][0]))  
  # (viewed,(movie id ,title))
  # output ==> [(129, u'##2027:::Mafia! (1998)'), 
  final2RDD=final1RDD.sortByKey(ascending=False) #sort on viewed descending
  #output ==> [(3428, u'##2858:::American Beauty (1999)'),
  f=open(sys.argv[3],"w")
  #os.remove(sys.argv[3])
   
  count=0;
  for i in final2RDD.collect():  #now its python object and not RDD
    count=count+1;
    print(type(i[0]))
    print(type(i[1]))
    
    f.write(`i[0]`+str(i[1])+'\n')  #outout => 3428##2858:::American Beauty (1999)
    #print (`i[0]`+str(i[1]))
    if count>9:  
       break




#ass2
#====
import sys
#rm -rf /home/cloudera/Desktop/ml-1m/1
#touch /home/cloudera/Desktop/ml-1m/1
#spark-submit --master local /home/cloudera/Desktop/ml-1m/Ass1.py /home/cloudera/Desktop/ml-1m/ratings.dat /home/cloudera/Desktop/ml-1m/movies.dat /home/cloudera/Desktop/ml-1m/1

from pyspark import SparkContext,SparkConf
import os

if __name__ == "__main__":

  conf=SparkConf().setAppName("Most watched movie")
  sc=SparkContext(conf=conf)

  ratingsRDD=sc.textFile(sys.argv[1]).map(lambda x : (x.split("::")[1],x.split("::")[2]))  # output ==> [(u'1193', u'5'), (u'661', u'3')
  #works on key so calculation should be on value
  ratings1RDD=ratingsRDD.combineByKey(lambda x:(x,1),    # attach 1 with value will work if not added 1 also might work for avg calculation
                                      lambda x,y : (int(x[0])+int(y) , x[1]+1),   # add ratings with previous value, add 1 
                                      lambda x,y : (int(x[0])+int(y[0]),x[1]+y[1]))   # add rating+count for all keys
  #output from above ==> [(u'593', (11219, 2578)), (u'1200', (7509, 1820)),
  ratings11RDD=ratings1RDD.filter(lambda x: x[1][1] >= 40)   # filter only review >=40
  # output ==> [(u'593', (11219, 2578)), (u'1200', (7509, 1820))
  ratings2RDD=ratings11RDD.map(lambda x: (str(x[0]),float(x[1][0])/float(x[1][1])))  # ==>[('593', 4.3518231186966645), ('1200', 4.1258241758241763),
  # out ==> [('593', 4.3518231186966645), ('1200', 4.1258241758241763),
  moviesRDD=sc.textFile(sys.argv[2]).map(lambda x:x.split("::"))
  movies1RDD=moviesRDD.map(lambda x:(x[0],x[1]))   # ==> [(u'1', u'Toy Story (1995)'), (u'2', u'Jumanji (1995)')
  
  finalRDD=movies1RDD.join(ratings2RDD)  # (movie id ,(title,avg score))  => u'1869'##Black Dog (1998):::2.3137254902 or
  # from command line output ==> [(u'1869', (u'Black Dog (1998)', 2.3137254901960786)),
  #finalRDD.take(100)
  final1RDD=finalRDD.map(lambda x: (x[1][1],x[0]+":::"+x[1][0]))  # ==> [(2.3137254901960786, u'1869:::Black Dog (1998)'), 
   # (avg score,(movie id ,title))
  #final1RDD.collect()
  final2RDD=final1RDD.sortByKey(ascending=False) #sort on avg rating descending   ==> [(5.0, u'1830:::Follow the Bitch (1998)'), (5.0, u'3656:::Lured (1947)'),
  
  f=open(sys.argv[3],"w")
  #os.remove(sys.argv[3])
   
  count=0;
  for i in final2RDD.collect():  #now its python object and not RDD
    count=count+1;
    
    f.write(`i[0]`+"##"+str(i[1])+'\n') 
    #f.write(`i[0]`+"##"+str(i[1][0])+":::"+str(i[1][1])+'\n')
    if count>19:  
       break

  #cound also be top(20,key=lambda x:-x[0])


#ass3
#====
import sys
#rm -rf /home/cloudera/Desktop/ml-1m/1
#touch /home/cloudera/Desktop/ml-1m/1
#spark-submit --master local /home/cloudera/Desktop/ml-1m/Ass1.py /home/cloudera/Desktop/ml-1m/ratings.dat /home/cloudera/Desktop/ml-1m/movies.dat /home/cloudera/Desktop/ml-1m/users.dat /home/cloudera/Desktop/ml-1m/6

from pyspark import SparkContext,SparkConf
import os

if __name__ == "__main__":

  conf=SparkConf().setAppName("Most watched movie")
  sc=SparkContext(conf=conf)

  # define profession in case statement
  def prof(x):
    return{
        '0' :  '"other" or not specified',
        '1':  "academic/educator",
	'2' :  "artist",
	'3':  "clerical/admin",
	'4':  "college/grad student",
	'5':  "customer service",
	'6':  "doctor/health care",
	'7':  "executive/managerial",
	'8':  "farmer",
	'9':  "homemaker",
	'10':  "K-12 student",
	'11':  "lawyer",
	'12':  "programmer",
	'13':  "retired",
	'14':  "sales/marketing",
	'15':  "scientist",
	'16':  "self-employed",
	'17':  "technician/engineer",
	'18':  "tradesman/craftsman",
	'19':  "unemployed",
	'20':  "writer"
        }.get(x,'NA')
    
  # define agegroup
  def agegroup(x):
    return{
         '1':  "Under 18",
	 '18':  "18-35",
	 '25':  "18-35",
	 '35':  "36-50",
	 '45':  "36-50",
	 '50':  "50+",
	 '56':  "50+"
        }.get(x,'NA')

  
  usersRDD=sc.textFile(sys.argv[3]).map(lambda x : (x.split("::")[0],x.split("::")[2]+":::"+x.split("::")[3]))  # output ==> [(u'1', u'1:::10'), (u'2', u'56:::16')]
  users1RDD=usersRDD.map(lambda x : (x[0],agegroup(x[1].split(":::")[0])+":::"+prof(x[1].split(":::")[1])))  # output ==> [(u'1', 'Under 18:::K-12 student'), ...
  #output userid#agegroup,profession
  #join with raating
  ratingsRDD=sc.textFile(sys.argv[1]).map(lambda x : (x.split("::")[0],x.split("::")[1]+":::"+x.split("::")[2]))  # output ==> [(u'1', u'1193:::5'), 
  joinusersratingsRDD=ratingsRDD.join(users1RDD)   # output ==> [(u'1867', (u'2987:::1', '50+:::executive/managerial')),  //userid#movieid,rating..,age,occupation
  #restructure to movieid#occupation,age,rating
  joinusersratings1RDD=joinusersratingsRDD.map(lambda x:(x[1][0].split(":::")[0],x[1][1].split(":::")[1]+":::"+x[1][1].split(":::")[0]+":::"+x[1][0].split(":::")[1]))
 
  # join with movies 
  moviesRDD=sc.textFile(sys.argv[2]).map(lambda x : (x.split("::")[0],x.split("::")[2]))  # [(u'1', u"Animation|Children's|Comedy")

  
  joinRDD=joinusersratings1RDD.join(moviesRDD)  # output [(u'3922', (u'lawyer:::36-50:::1', u'Comedy')),  //movieid#occupation,age,rating..,genre
  # restructure to calculate average  ==> occupation,agegroup,genre#rating
  join1RDD=joinRDD.map(lambda x: (x[1][0].split(":::")[0]+":::"+x[1][0].split(":::")[1]+":::"+x[1][1],x[1][0].split(":::")[2]))
  #output from above ==> [(u'lawyer:::36-50:::Comedy', u'1'),

  calculateRDD=join1RDD.combineByKey(lambda x: (x,1),
                                    lambda x,y : (int(x[0])+int(y),int(x[1])+1), 
                                    lambda x,y:(int(x[0])+int(y[0]),int(x[1])+int(y[1])))
    
  #output from above [(u'clerical/admin:::36-50:::Action|Adventure|Romance|War', (84, 19)), (u'customer service:::36-50:::Action|Adventure|Sci-Fi|War', (22, 9))]

  #get avg
  calavg=calculateRDD.map(lambda x: (x[0],float(x[1][0])/float(x[1][1])))
  #output from above==> [(u'clerical/admin:::36-50:::Action|Adventure|Romance|War', 4.4210526315789478), (u'customer service:::36-50:::Action|Adventure|Sci-Fi|War', 2.4444444444444446)
  #change key and value: profession,agegroup,rating#genre
  calavg1RDD=calavg.map(lambda x: (x[0].split(":::")[0]+":::"+x[0].split(":::")[1]+":::"+str(x[1]),x[0].split(":::")[2] ))
  # [(u'clerical/admin:::36-50:::4.42105263158', u'Action|Adventure|Romance|War'), (u'customer service:::36-50:::2.44444444444', u'Action|Adventure|Sci-Fi|War')]

  sortavgRDD=calavg1RDD.sortByKey(False)  
  #[(u'writer:::50+:::5.0', u'Documentary|Drama'), (u'writer:::50+:::5.0', u'Adventure|Animation|Sci-Fi')]

  
  sortavg1RDD=sortavgRDD.map(lambda x: (x[0].split(":::")[0]+":::"+x[0].split(":::")[1],x[1]))
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

