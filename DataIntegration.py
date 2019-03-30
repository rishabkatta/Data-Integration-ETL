'''
@author-name: Rishab Katta

Python program for creating sources using views on existing imdb relational database,
creating gav mappings on the sources and performing querying those mappings

This program uses the IMDB Database created from the SQL dump provided Prof. Mior
'''

import psycopg2
import gzip
import shutil
import time


class DatabaseConnection:

    def __init__(self,h,db,username,pwd):
        '''
        Constructor is used to connect to the database
        :param h: hostname
        :param db: database name
        :param username: Username
        :param pwd: password
        '''
        try:
            self.connection = psycopg2.connect(host=str(h), database=str(db), user=str(username), password=str(pwd))
            # self.connection = psycopg2.connect(host='localhost', database='postgres', user='user006', password='abcde')
            self.connection.autocommit=True
            self.cursor=self.connection.cursor()
        except Exception as e:
            print(getattr(e, 'message', repr(e)))
            print(getattr(e, 'message', str(e)))

    def create_views(self):
        '''
        Funtion used to create sources mentioned in the assignment
        :return: None
        '''

        #non-materialized views

        self.cursor.execute("create view ComedyMovie as "
                            " select m.id, m.title, m.startyear as year from movie m inner join movie_genre mg on mg.movie=m.id "
                            " inner join genre g on g.id = mg.genre where m.runtime >= 75 and m.type ilike 'movie' and g.name ilike 'comedy' ")

        self.cursor.execute("create view NonComedyMovie as "
                            " select m.id, m.title, m.startyear as year from movie m inner join movie_genre mg on mg.movie=m.id "
                           " inner join genre g on g.id = mg.genre where m.runtime >= 75 and m.type ilike 'movie' and g.name not ilike 'comedy' ")

        self.cursor.execute("create view ComedyActor as "
                            " select mb.id, mb.name, mb.birthyear, mb.deathyear from member mb inner join movie_actor ma on ma.actor=mb.id "
                            " inner join movie m on m.id= ma.movie inner join movie_genre mg on mg.movie = m.id inner join genre g on g.id = mg.genre "
                            " where g.name ilike 'comedy'")

        self.cursor.execute("create view NonComedyActor as "
                            " select mb.id, mb.name, mb.birthyear, mb.deathyear from member mb inner join movie_actor ma on ma.actor=mb.id "
                            " inner join movie m on m.id= ma.movie inner join movie_genre mg on mg.movie = m.id inner join genre g on g.id = mg.genre "
                            " where g.name not ilike 'comedy'")

        self.cursor.execute("create view ActedIn as "
                            " select actor, movie from movie_actor")

        #materialized views

        self.cursor.execute("create materialized view ComedyMovieMV as "
                            " select m.id, m.title, m.startyear as year from movie m inner join movie_genre mg on mg.movie=m.id "
                            " inner join genre g on g.id = mg.genre where m.runtime >= 75 and m.type ilike 'movie' and g.name ilike 'comedy' ")

        self.cursor.execute("create materialized view NonComedyMovieMV as "
                            " select m.id, m.title, m.startyear as year from movie m inner join movie_genre mg on mg.movie=m.id "
                            " inner join genre g on g.id = mg.genre where m.runtime >= 75 and m.type ilike 'movie' and g.name not ilike 'comedy' ")

        self.cursor.execute("create materialized view ComedyActorMV as "
                            " select mb.id, mb.name, mb.birthyear, mb.deathyear from member mb inner join movie_actor ma on ma.actor=mb.id "
                            " inner join movie m on m.id= ma.movie inner join movie_genre mg on mg.movie = m.id inner join genre g on g.id = mg.genre "
                            " where g.name ilike 'comedy'")

        self.cursor.execute("create materialized view NonComedyActorMV as "
                            " select mb.id, mb.name, mb.birthyear, mb.deathyear from member mb inner join movie_actor ma on ma.actor=mb.id "
                            " inner join movie m on m.id= ma.movie inner join movie_genre mg on mg.movie = m.id inner join genre g on g.id = mg.genre "
                            " where g.name not ilike 'comedy'")

        self.cursor.execute("create materialized view ActedInMV as "
                            " select actor, movie from movie_actor")

    def gav_mappings(self):
        '''
        Funtion used to create GAV mappings on the sources created in the above funtion
        :return: None
        '''

        self.cursor.execute("create view All_Movie as "
                            " select cm.*, 'comedy' AS genre from comedymovie cm "
                            " union "
                            " select ncm.*, 'non-comedy' AS genre from noncomedymovie ncm ")

        self.cursor.execute("create view All_Actor as "
                            " select ca.* from comedyactor ca "
                            " union "
                            " select nca.* from noncomedyactor nca ")

        self.cursor.execute("create view All_Movie_Actor as "
                            " select ai.* from actedin ai")

    def queries_gav(self):
        '''
        Querying on the GAV mapping
        :return: None
        '''

        self.cursor.execute("select aa.id from all_actor aa inner join all_movie_actor ama on aa.id = ama.actor inner join all_movie am on ama.movie = am.id "
                            " where aa.deathyear is null and am.year<=2005 and am.year>=2000 group by aa.id having count(am.id)>10 ")

        self.cursor.execute("select distinct aa.id from all_actor aa inner join all_movie_actor ama on aa.id=ama.actor inner join all_movie am on ama.movie = am.id "
                            " where aa.name ilike 'ja%' and am.genre ilike 'non-comedy' ")

    def queries_expanded(self):

        '''
        GAV mappings expanded in the queries
        :return: None
        '''

        '''
        Time taken for 3.1 using non-materialized views
        
        Successfully run. Total query runtime: 39.2812 secs.
        388 rows affected.
        '''
        start_time = time.time()
        self.cursor.execute("select aa.id from (select ca.* from comedyactor ca union select nca.* from noncomedyactor nca) aa "
                            " inner join (select ai.* from actedin ai) ama on aa.id = ama.actor inner join "
                            " (select cm.*, 'comedy' AS genre from comedymovie cm union select ncm.*, 'non-comedy' AS genre from noncomedymovie ncm) am "
                            " on ama.movie = am.id where aa.deathyear is null and am.year<=2005 and am.year>=2000 group by aa.id having count(am.id)>10 ")

        print("--- %s seconds for 3.1 using non-materialized views ---" % (time.time() - start_time))

        '''
        Time taken for 3.1 using materialized views

        Successfully run. Total query runtime: 26.0568 secs.
        388 rows affected.
        '''
        start_time = time.time()
        self.cursor.execute("select aa.id from (select ca.* from comedyactorMV ca union select nca.* from noncomedyactorMV nca) aa "
                            " inner join (select ai.* from actedinMV ai) ama on aa.id = ama.actor inner join "
                            " (select cm.*, 'comedy' AS genre from comedymovieMV cm union select ncm.*, 'non-comedy' AS genre from noncomedymovieMV ncm) am "
                            " on ama.movie = am.id where aa.deathyear is null and am.year<=2005 and am.year>=2000 group by aa.id having count(am.id)>10 ")

        print("--- %s seconds for 3.1 using materialized views ---" % (time.time() - start_time))

        '''
        Time taken for 3.2 using non-materialized views
        
        Successfully run. Total query runtime: 27.0963 secs.
        5610 rows affected.
        '''
        start_time = time.time()
        self.cursor.execute("select distinct aa.id from (select ca.* from comedyactor ca union select nca.* from noncomedyactor nca) aa "
                            " inner join (select ai.* from actedin ai) ama on aa.id=ama.actor inner join "
                            " (select cm.*, 'comedy' AS genre from comedymovie cm union select ncm.*, 'non-comedy' AS genre from noncomedymovie ncm) am "
                            " on ama.movie = am.id where aa.name ilike 'ja%' and am.genre ilike 'non-comedy' ")

        print("--- %s seconds for 3.2 using non-materialized views ---" % (time.time() - start_time))

        '''
        Time taken for 3.2 using materialized views
        
        Successfully run. Total query runtime: 6.10839 secs.
        5610 rows affected.
        '''
        start_time = time.time()
        self.cursor.execute("select distinct aa.id from (select ca.* from comedyactorMV ca union select nca.* from noncomedyactorMV nca) aa "
                            " inner join (select ai.* from actedinMV ai) ama on aa.id=ama.actor inner join "
                            " (select cm.*, 'comedy' AS genre from comedymovieMV cm union select ncm.*, 'non-comedy' AS genre from noncomedymovieMV ncm) am "
                            " on ama.movie = am.id where aa.name ilike 'ja%' and am.genre ilike 'non-comedy' ")

        print("--- %s seconds for 3.2 using materialized views ---" % (time.time() - start_time))

    def queries_optimized(self):
        '''
        Queries optimized by removing unnecesary columns and joins from the expanded queries
        :return: None
        '''

        '''
        For 3.1 optimized using non materialized views
        
        Successfully run. Total query runtime: 35.98355 secs.
        388 rows affected.
        '''
        start_time = time.time()
        self.cursor.execute("select aa.id from (select ca.id, ca.deathyear from comedyactor ca union select nca.id, nca.deathyear from noncomedyactor nca) aa "
                            " inner join actedin ama on aa.id = ama.actor inner join "
                            " (select cm.id, cm.year, 'comedy' AS genre from comedymovie cm union select ncm.id, ncm.year, 'non-comedy' AS genre from noncomedymovie ncm) am "
                            " on ama.movie = am.id where aa.deathyear is null and am.year between 2000 and 2005 group by aa.id having count(am.id)>10 ")

        print("--- %s seconds for 3.1 optimized using non-materialized views ---" % (time.time() - start_time))

        '''
        For 3.1 optimized using materialized views

        Successfully run. Total query runtime: 10.7747 secs.
        388 rows affected.
        '''
        start_time = time.time()
        self.cursor.execute("select aa.id from (select ca.id, ca.deathyear from comedyactorMV ca union select nca.id, nca.deathyear from noncomedyactorMV nca) aa "
                            " inner join actedin ama on aa.id = ama.actor inner join "
                            " (select cm.id, cm.year, 'comedy' AS genre from comedymovieMV cm union select ncm.id, ncm.year, 'non-comedy' AS genre from noncomedymovieMV ncm) am "
                            " on ama.movie = am.id where aa.deathyear is null and am.year between 2000 and 2005 group by aa.id having count(am.id)>10 ")

        print("--- %s seconds for 3.1 optimized using materialized views ---" % (time.time() - start_time))

        '''
        For 3.2 optimized using non materialized views
        Successfully run. Total query runtime: 160.9411 secs.(I don't understand why its taking longer.I've tried but I can't. It could be because of cache.)
        5610 rows affected.
        '''
        start_time = time.time()
        self.cursor.execute("select distinct aa.id from noncomedyactor aa inner join actedin ama on aa.id=ama.actor "
                            " inner join noncomedymovie am on ama.movie = am.id where aa.name ilike 'ja%'")

        print("--- %s seconds for 3.2 optimized using non-materialized views ---" % (time.time() - start_time))

        '''
        For 3.2 optimized using materialized views
        Successfully run. Total query runtime:4.70409 secs.
        5610 rows affected.
        '''
        start_time = time.time()
        self.cursor.execute("select distinct aa.id from noncomedyactorMV aa inner join actedinMV ama on aa.id=ama.actor "
                            " inner join noncomedymovieMV am on ama.movie = am.id where aa.name ilike 'ja%' ")

        print("--- %s seconds for 3.2 optimized using materialized views ---" % (time.time() - start_time))

if __name__ == '__main__':
    h = str(input("Enter host name"))
    db = str(input("Enter Database Name"))
    username = str(input("Enter username"))
    pwd = str(input("Enter password"))

    database_connection = DatabaseConnection(h,db,username,pwd)
    database_connection.create_views()
    database_connection.gav_mappings()
    database_connection.queries_gav()
    database_connection.queries_expanded()
    database_connection.queries_optimized()