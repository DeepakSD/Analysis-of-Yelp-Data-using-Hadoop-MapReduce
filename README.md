# Analysis-of-Yelp-Data-using-Hadoop-MapReduce-and-Spark

Author: Deepak Shanmugam

#Dataset Description:

The dataset comprises of three csv files, namely user.csv, business.csv and review.csv.  

Business.csv file contain basic information about local businesses. 
Business.csv file contains the following columns "business_id"::"full_address"::"categories"
'business_id': (a unique identifier for the business)
'full_address': (localized address), 
'categories': [(localized category names)]  

review.csv file contains the star rating given by a user to a business. Use user_id to associate this review with others by the same user. Use business_id to associate this review with others of the same business. 
review.csv file contains the following columns "review_id"::"user_id"::"business_id"::"stars"
'review_id': (a unique identifier for the review)
'user_id': (the identifier of the reviewed business), 
'business_id': (the identifier of the authoring user), 
'stars': (star rating, integer 1-5),the rating given by the user to a business

user.csv file contains aggregate information about a single user across all of Yelp
user.csv file contains the following columns "user_id"::"name"::"url"
'user_id': (unique user identifier), 
'name': (first name, last initial, like 'Matt J.'), this column has been made anonymous to preserve privacy 
'url': url of the user on yelp


#Problem Description:

Problem 1: Lists the unique categories of business located in “Palo Alto”.

Problem 2: Finds the top ten rated businesses using the average ratings.

Problem 3: Lists the  business_id , full address and categories of the Top 10 businesses using the average ratings.
This problem is addressed using Reduce side join and job chaining technique.

Problem 4: Lists the 'user id' and 'rating' of users that reviewed businesses located in Stanford.
This problem is addressed using In Memory join technique using Distributed Cache.


#How to execute:

------Problem 1------
hadoop jar problem1.jar yelp.UniqueBusiness <input filepath(business.csv)> <output filepath(output1)>

------Problem 2------
hadoop jar problem2.jar yelp.AverageRating <input filepath(review.csv)> <output filepath(output2)>

------Problem 3------
hadoop jar problem3.jar yelp.Reducejoin <input filepath1(business.csv)> <input filepath2(review.csv)> <output filepath(output3)>

------Problem 4------
hadoop jar problem4.jar yelp.MapJoin <distributed cache filepath(business.csv)> <input filepath(review.csv)> <output filepath(output4)>

