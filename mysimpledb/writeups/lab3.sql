/* COSC 460 Fall 2014, Lab 3 */

/* These set the output format.  Please be sure to leave these settings as is. */
.header OFF
.mode list 

/* For each of the queries below, put your SQL in the place indicated by the comment.  
   Be sure to have all the requested columns in your answer, in the order they are 
   listed in the question - and be sure to sort things where the question requires 
   them to be sorted, and eliminate duplicates where the question requires that.   
   I will grade the assignment by running the queries on a test database and 
   eyeballing the SQL queries where necessary.  I won't grade on SQL style, but 
   we also won't give partial credit for any individual question - so you should be 
   confident that your query works. In particular, your output should match 
   the example output.
*/

/* Q1 -  Find the titles of all movies directed by Steven Spielberg.  */
select " ";
select "Q1";

/* Put your SQL for Q1 below */
select title
from Movie
where director="Steven Spielberg";

/* Q2 -  Find all years that have a movie that received a rating of 4 or 5, 
         and sort them in increasing order.             
*/
select " ";
select "Q2";

/* Put your SQL for Q2 below */
select distinct year
from Movie M, Rating RT
where M.mID = RT.mID and stars >= 4 order by year;

/* Q3 -  Find the titles of all movies that have no ratings.
*/
select " ";
select "Q3";

/* Put your SQL for Q3 below */
select title
from Movie
where title not in (select title
			  	  	from Movie M, Rating R
			  	  	where M.mID = R.mID);

/* Q4 -  Write a query to return the ratings data in a more 
         readable format: reviewer name, movie title, stars, and ratingDate. 
         Also, sort the data, first by reviewer name, then by movie title, 
         and lastly by number of stars, all in ascending order.
*/
select " ";
select "Q4";

/* Put your SQL for Q4 below */
select name, title, stars, ratingDate
from Movie M, Rating RT, Reviewer RV
where M.mID = RT.mID and RT.rID = RV.rID order by name, title, stars;

/* Q5 -  For all cases where the same reviewer rated the same movie twice 
         and gave it a higher rating the second time, return the reviewer's 
         name and the title of the movie.
*/
select " ";
select "Q5";

/* Put your SQL for Q5 below */
select name, title
from (select RT1.mID, RT1.rID
	  from Rating RT1, Rating RT2
	  where RT1.mID = RT2.mID and
			RT1.rID = RT2.rID and
			RT1.stars > RT2.stars and
			RT1.ratingDate > RT2.ratingDate) RT,
	 Movie M, Reviewer RV
where M.mID = RT.mID and RT.rID = RV.rID;

/* Q6 - For each movie that has at least one rating, find the highest number 
        of stars that movie received. Return the movie title and number of 
        stars. Sort by movie title. 
*/
select " ";
select "Q6";

/* Put your SQL for Q6 below */
select title, max_stars
from Movie M, (select mID, max(stars) as max_stars
			   from Rating group by mID) RT
where M.mID = RT.mID order by title;

/* Q7 - For each movie, the title along with the number of ratings it has 
        received.  Your result should include those movies that have zero ratings.                                                                 
*/
select " ";
select "Q7";

/* Put your SQL for Q7 below */
select title, no_ratings
from Movie M join (select mID, count(*) as no_ratings from Rating group by mID) CRT
			 on M.mID = CRT.mID
union
select title, 0 from Movie where mID not in (select mID from Rating);


/* Q8 - For each movie that has at least one rating, return the title and the 
        'rating spread', that is, the difference between highest and lowest 
        ratings given to that movie. Sort by rating spread from highest to 
        lowest, then by movie title alphabetically.   
*/
select " ";
select "Q8";

/* Put your SQL for Q8 below */
select title, rating_spread
from Movie M, (select mID, max(stars) - min(stars) as rating_spread
			   from Rating group by mID) RT
where M.mID = RT.mID order by rating_spread desc, title;

/* Q9 -  Find the difference between the average rating of movies released before 
         1980 and the average rating of movies released after 1980. (Make sure to 
         calculate the average rating for each movie, then the average of those 
         averages for movies before 1980 and movies after. Don't just calculate 
         the overall average rating before and after 1980.)  
*/
select " ";
select "Q9";

/* Put your SQL for Q9 below */
select abs(avg_post_1980 - avg_pre_1980)
from (select avg(avg_star) as avg_pre_1980
	  from Movie M, (select mID, avg(stars) as avg_star
	  				 from Rating group by mID) RT
	  where M.mID = RT.mID and M.year < 1980),
	 (select avg(avg_star) as avg_post_1980
	  from Movie M, (select mID, avg(stars) as avg_star
	  			 	from Rating group by mID) RT
	  where M.mID = RT.mID and M.year >= 1980);


/* Q10 - For each director, return the director's name together with the title(s) 
         of the movie(s) they directed that received the highest rating among all 
         of their movies, and the value of that rating. 
*/
select " ";
select "Q10";

/* Put your SQL for Q10 below */
select distinct M.director, M.title, RT.stars
from Movie M, Rating RT, (select M.mID, director, max(stars) as max_stars
						  from Movie M, Rating RT
						  where M.mID = RT.mID group by director) MRT
where M.mID = MRT.mID and M.director = MRT.director and RT.stars = MRT.max_stars;
