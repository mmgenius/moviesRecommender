package com.camillepradel.movierecommender.model.db;

import com.camillepradel.movierecommender.model.Genre;
import com.camillepradel.movierecommender.model.Movie;
import com.camillepradel.movierecommender.model.Rating;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;



public class MongodbDatabase extends AbstractDatabase {
	
	DB database = null;
	
	public static void main(String[] args) {
		MongodbDatabase db = new MongodbDatabase();
		//db.getRatingsFromUser(3);
		Movie m = new Movie(1089, "", null);
		db.addOrUpdateRating(new Rating(m,6038, -100));
	}
	
	MongoClient mongoClient = null;
	
    public MongodbDatabase() {
		super();
		try {
			MongoClient mongoClient = new MongoClient(new MongoClientURI(url));
			database = mongoClient.getDB("MovieLens");
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

    // db connection info
    String url = "mongodb://localhost:27017";
	
    @Override
    public List<Movie> getAllMovies() {
        List<Movie> movies = new LinkedList<Movie>();

        if (database != null) {
        	DBCollection collection = database.getCollection("movies");
        	DBCursor cursor = collection.find();
        	while(cursor.hasNext()) {
        		DBObject dbmovie = cursor.next();
        		BasicDBList genresDBList = (BasicDBList) dbmovie.get("genres");
        		List<Genre> movieGenres = new LinkedList<>();
        		for (int i = 0; i < genresDBList.size(); i++) {
        			Genre g = new Genre(-1, genresDBList.get(i).toString());
        			//System.out.println(genresDBList.get(i).toString());
        			movieGenres.add(g);
        		}
        		//System.out.println("id: " +dbmovie.get("_id").toString()+" titre: " + dbmovie.get("title").toString());
        	    movies.add(new Movie(Integer.parseInt(dbmovie.get("_id").toString()), dbmovie.get("title").toString(), movieGenres));
        	}    	
        }
        return movies;
    }

	@Override
    public List<Movie> getMoviesRatedByUser(int userId) {
    	//List to be filled and returned
        List<Movie> movies = new LinkedList<Movie>(); 	
        if (database != null) {
        	//search user with id in collection users
        	DBCollection collection = database.getCollection("users");
        	BasicDBObject query = new BasicDBObject();
        	query.put("_id", userId);
        	DBCursor cursor = collection.find(new BasicDBObject("_id", userId));
        	DBObject userObject = cursor.one();

        	BasicDBList moviesDBList = (BasicDBList) userObject.get("movies");
        	for (int i = 0; i < moviesDBList.size(); i++) {
        		//get a movie
        		BasicDBObject movieRated = (BasicDBObject) moviesDBList.get(i);
        		//create a movie object
        		Movie m = getMovieById((int) movieRated.get("movieid"));
        		movies.add(m);
        	}
        }
        return movies;
    }

    private Movie getMovieById(int id) {
    	if (database != null) {
        	DBCollection collection = database.getCollection("movies");
        	DBCursor cursor = collection.find(new BasicDBObject("_id", id));
        	DBObject movieObject = cursor.one();
        	List<Genre> movieGenres = new LinkedList<>();
        	BasicDBList genresDBList = (BasicDBList) movieObject.get("genres");
    		for (int i = 0; i < genresDBList.size(); i++) {
    			Genre g = new Genre(-1, genresDBList.get(i).toString());
    			movieGenres.add(g);
    		}
        	return (new Movie(Integer.parseInt(movieObject.get("_id").toString()), movieObject.get("title").toString(), movieGenres));
    	}
		return null;
	}

	@Override
    public List<Rating> getRatingsFromUser(int userId) {
        // TODO: write query to retrieve all ratings from user with id userId
		// get from users collection the user object, then get movies associated to it
		// get rating and movie id, get movie and create for each object in array a rating
		List<Rating> ratings = new LinkedList<Rating>();
		if (database != null) {
	    	DBCollection collection = database.getCollection("users");
	    	BasicDBObject query = new BasicDBObject();
	    	query.put("_id", userId);
	    	DBCursor cursor = collection.find(new BasicDBObject("_id", userId));
	    	DBObject userObject = cursor.one();	
        	BasicDBList moviesDBList = (BasicDBList) userObject.get("movies");
        	for (int i = 0; i < moviesDBList.size(); i++) {
        		//get a movie
        		BasicDBObject movieRated = (BasicDBObject) moviesDBList.get(i);
        		//get rating
        		int score = movieRated.getInt("rating");
        		//create a movie object
        		Movie m = getMovieById((int) movieRated.get("movieid"));
        		ratings.add(new Rating(m,userId, score));
        	} 
		}
		
        
        Genre genre0 = new Genre(0, "genre0");
        Genre genre1 = new Genre(1, "genre1");
        ratings.add(new Rating(new Movie(0, "Titre 0", Arrays.asList(new Genre[]{genre0, genre1})), userId, 3));
        ratings.add(new Rating(new Movie(2, "Titre 2", Arrays.asList(new Genre[]{genre1})), userId, 4));
        return ratings;
    }

    @Override
    public void addOrUpdateRating(Rating rating) {
        //         - add rating between specified user and movie if it doesn't exist
        //         - update it if it does exist
    	//concerned collection: users. Get by user id:
    	//db.users.find({_id:<userid>, movies: {$elemMatch:{movieid:<movieid>}}}): if empty, rating does not exist
    	
    	if (database != null) {
        	//search user with id in collection users
        	DBCollection collection = database.getCollection("users");
        	BasicDBObject andQuery = new BasicDBObject();
        	List<BasicDBObject> obj = new ArrayList<BasicDBObject>();
        	obj.add(new BasicDBObject("_id", rating.getUserId()));
        	obj.add(new BasicDBObject("movies", new BasicDBObject("$elemMatch", new BasicDBObject("movieid",rating.getMovieId()))));
        	andQuery.put("$and", obj);
        	DBCursor cursor = collection.find(andQuery);
        	DBObject userObject = cursor.one();
        	if(userObject!=null) {
        		//item exists, updating
        		System.out.println("update");
        		//db.users.update({_id:6038, "movies.movieid":1079}, {$set: {"movies.$.rating":10}}, false, true)
        		BasicDBObject updateFindQuery = new BasicDBObject();
        		List<BasicDBObject> args = new ArrayList<BasicDBObject>();
        		args.add(new BasicDBObject("_id", rating.getUserId()));
        		args.add(new BasicDBObject("movies.movieid", rating.getMovieId()));
        		updateFindQuery.put("$and", args);
        		BasicDBObject updateSetQuery = new BasicDBObject();
        		BasicDBObject arrayObj = new BasicDBObject("movies.$.rating", rating.getScore());
        		updateSetQuery.put("$set", arrayObj);
        		collection.update(updateFindQuery, updateSetQuery);
        	} else {
        		//item does not exist, creating
        		//db.users.update({_id:6038}, {$push : {"movies" : {'movieid' : 1082 , 'rating' : 33, "timestamp": Math.round(new Date()/1000), "date":new Date() } }}, false, true)
        		System.out.println("create");
        		Date date = new Date();
        		DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        		BasicDBObject newItem = new BasicDBObject();
        		newItem.append("movieid", rating.getMovieId());
        		newItem.append("rating", rating.getScore());
        		newItem.append("timestamp", Math.round(date.getTime()/1000));
        		newItem.append("date", sdf.format(date));
        		
        		BasicDBObject updateQuery = new BasicDBObject("$push", new BasicDBObject("movies", newItem));
        		collection.update(new BasicDBObject("_id", rating.getUserId()), updateQuery);
        	}
        	
    	}

    }

    @Override
    public List<Rating> processRecommendationsForUser(int userId, int processingMode) {
        // TODO: process recommendations for specified user exploiting other users ratings
        //mongo db find similar documents db.users.find({"_id":6038}, {movies:1})
    	//       use different methods depending on processingMode parameter
        Genre genre0 = new Genre(0, "genre0");
        Genre genre1 = new Genre(1, "genre1");
        Genre genre2 = new Genre(2, "genre2");
        List<Rating> recommendations = new LinkedList<Rating>();
        String titlePrefix;
        if (processingMode == 0) {
            titlePrefix = "0_";
        } else if (processingMode == 1) {
            titlePrefix = "1_";
        } else if (processingMode == 2) {
            titlePrefix = "2_";
        } else {
            titlePrefix = "default_";
        }
        recommendations.add(new Rating(new Movie(0, titlePrefix + "Titre 0", Arrays.asList(new Genre[]{genre0, genre1})), userId, 5));
        recommendations.add(new Rating(new Movie(1, titlePrefix + "Titre 1", Arrays.asList(new Genre[]{genre0, genre2})), userId, 5));
        recommendations.add(new Rating(new Movie(2, titlePrefix + "Titre 2", Arrays.asList(new Genre[]{genre1})), userId, 4));
        recommendations.add(new Rating(new Movie(3, titlePrefix + "Titre 3", Arrays.asList(new Genre[]{genre0, genre1, genre2})), userId, 3));
        return recommendations;
    }    
}
