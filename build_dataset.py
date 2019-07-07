### Retrieve 

## import libraries and set defaults
# import required libraries
import requests as requests
import numpy as np
import pandas as pd
import time as time
import math as math
import pickle as pickle
from bs4 import BeautifulSoup

# base URL for the BGG XML API2 with str.format() replacement field
APIbase = "https://www.boardgamegeek.com/xmlapi2/thing?id={}&ratingcomments=1"

# full URL for retrieving a rating with str.format() replacement fields
APIfull = "https://www.boardgamegeek.com/xmlapi2/thing?id={}&stats=1&ratingcomments=1&page={}"

# URL for retrieving user data with str.format() replacement field
APIuser = "https://www.boardgamegeek.com/xmlapi2/user?name={}"



# The BGG database contains a large number of non-boardgame items as well as a lot of boardgames that exist somewhat 
# theoretically and thus have few if any ratings. I am choosing to limit the population here to the top 2500 ranked 
# games, which excludes the above and makes building the dataset significantly simpler.

# We will first need to establish a list of the top 2500 ranked games to sample from. 
# The below script collects the BGG object ID for each via HTML scraping.

# Retrieved 20181117.


## adapting Erik's HMTL scraper below to loop through the first 25 pages of BGG's game rankings (2500 games)

# This script will store the game ids of the top 100 boardgames in a list called game ids.
# Written by: Erik Andersson Sunden (bgg username: pastej)

import urllib.request

#
# Accesses the Top 100 page and stores it locally as toplist.html
#
topgamesurl = "https://boardgamegeek.com/browse/boardgame/page/{}"
topgameids = []
listnumber = 1

# loop through the first 25 pages of the BGG rankings
for page in range(1, 26):
  # retrieve the raw html for each page
  urllib.request.urlretrieve(topgamesurl.format(str(page)), "toplist.html")
  
  # convert the html into raw text
  f = open("toplist.html", "r", encoding = "utf-8")
  lines = f.readlines()
  f.close()
  
  # find the BGG object ID of the currently ranked game in the HTML and append it to the list
  for linenr,line in enumerate(lines):
    #
    # Each list entry starts with the text string '<a name="'+str(listnumber)+'">'
    # where the listnumber is the current ranking
    # 
    i = line.find('<a name="' + str(listnumber) + '">')
    
    if i > 0:
      boardgameline = lines[linenr+5]
      
      index = boardgameline.find("/boardgame")
      
      boardgameline = boardgameline[index+11:]
      
      index = boardgameline.find("/")
      
      boardgamenr = int(boardgameline[:index])
      
      topgameids.append(boardgamenr)
      
      listnumber += 1
      


# Having assembled a list of BGG object IDs representing the top 2500 games (topgameids), 
# the next step is to determine the number of ratings that exist for each game.

# To avoid making 2500 separate API calls, I am batching the games into sets of 50.

# Retrieved 20181117.

# batch the games into groups of 50
batchedgames = [topgameids[i:i+50] for i in range(0, 2500, 50)]

# format for API requests (single comma separated string)
for i in range(0,len(batchedgames)):
  batchedgames[i] = ",".join(str(element) for element in batchedgames[i])

# initialize lists to hold the number of ratings
numberofratings = []

# loop through the list of batched game IDs
for batch in batchedgames:
  # make the API call
  address = APIbase.format(batch)
  requested_data = requests.get(address)
  
  # convert the returned XML into a soup object
  soup = BeautifulSoup(requested_data.text, "xml")
  
  # loop through the batched games contained in soup
  for game in soup.find_all("item"):
    # extract the number of ratings from the soup object and store
    numberofratings.append(game.comments["totalitems"])
  
  # pause briefly to avoid overtaxing the API
  time.sleep(5)



# We have now retrieved all of the information from the API necessary to generate the random sample. 
# My sampling strategy here will be to first sample with replacement all available game IDs weighted 
# by their total number of ratings. We will eventually wind up with a sample size of 10000, so we need 
# to begin with that many (non-unique) game IDs.

# bind the lists generated thus far into a dataframe
gamesdf = pd.DataFrame(data = {"ID" : topgameids, "Ratings" : numberofratings}, dtype = int)

# generate a new column representing a proportion of total ratings associated with each game
gamesdf["Weight"] = gamesdf["Ratings"]/gamesdf["Ratings"].sum()

# sample game IDs weighted by the proportion of total ratings
gamestosample = np.random.choice(gamesdf["ID"], size = 10000, replace = True, p = gamesdf["Weight"])

# bind the sampled game IDs into a new dataframe
sample = pd.DataFrame(data = {"ID" : gamestosample})

# count the number of comments to retrieve from each game ID
counts = sample["ID"].value_counts()



# Now that we know which games to sample from and how many times to sample each, we need to determine 
# which specific ratings to retrieve via the API.

# add an additional column to the sample dataframe to hold the selected rating numbers
sample["Rating Number"] = 0

# for each game, generate the number of random samples (without replacement) determined above from its available comments
for game in counts.keys():
  # take the determined number of random samples from the total number of comments available for each game
  # subset the sample data frame by game ID and store the resulting rating numbers there
  sample.loc[sample["ID"] == game, "Rating Number"] = np.random.choice(range(1, int(gamesdf["Ratings"][gamesdf["ID"] == game] + 1)), size = counts[game], replace = False)



# Here we retrieve the actual ratings form the API. At this point, we may wish to go back and retrieve 
# additional information from each fetched page in the future, so we will build a dictionary of soup 
# objects as we go and extract the ratings after the fact.

# Retrieved 2018116.

# declare a nested dictionary to hold the retrieved soup objects
# the "outside" key will be the BGG game ID
# the "inside" key will be the page number as defined below
BGGstew = {game: {} for game in gamesdf["ID"]}

# iterate over the rows of the sample dataframe
for index, row in sample.iterrows():
  # convert rating into the page number on which the rating is found
  pagenumber = math.floor(row["Rating Number"]/100)
  
  # check to see if the necessary page has already been retrieved in order to limit total API calls
  if (pagenumber not in BGGstew[row["ID"]].keys()):
    # call the API, inserting game number and the page number 
    address = APIfull.format(row["ID"], pagenumber)
    requested_data = requests.get(address)
    
    # convert the returned XML into a soup object and store it
    soup = BeautifulSoup(requested_data.text, "xml")
    BGGstew[row["ID"]][pagenumber] = soup

  # pause briefly to avoid overtaxing the API
  time.sleep(5)



# Now we pull the rating values alongside a variety of other potentially useful fields out of the 
# soup objects above into the sample dataframe.

# add additional columns to sample for the new fields
sample["Rating"] = np.nan
sample["Username"] = np.nan
sample["Name"] = ""
sample["Release Year"] = np.nan
sample["Weight"] = np.nan
sample["Owners"] = np.nan
sample["Average Rating"] = np.nan

# iterate over the rows of the sample dataframe
for index, row in sample.iterrows():
  # calculate page number as above
  pagenumber = math.floor(row["Rating Number"]/100)
  
  # the index for a given rating on a page is given by the final two digits of the rating number - 1
  ratingindex = row["Rating Number"] % 100 - 1
  
  # recall the soup object by providing the game ID and page ID
  soup = BGGstew[row["ID"]][pagenumber]
  
  # extract the desired rating value and username
  sample.loc[index, "Rating"] = soup.find_all("comment")[ratingindex]["rating"]
  sample.loc[index, "Username"] = soup.find_all("comment")[ratingindex]["username"]
  
  # while looping through, collect the boardgame name, release year, weight, number of owners, and average rating
  sample.loc[index, "Name"] = soup.find("name")["value"]
  sample.loc[index, "Release Year"] = soup.find("yearpublished")["value"]
  sample.loc[index, "Weight"] = soup.find("averageweight")["value"]
  sample.loc[index, "Owners"] = soup.find("owned")["value"]
  sample.loc[index, "Average Rating"] = soup.find("average")["value"]

# "pickle" BGGstew and the sample dataframe here for potentail recovery
# this requires raising the recursion limit
sys.setrecursionlimit(50000)

fileObject = open("Objects/20181118_BGGstew_pickle", "wb")
pickle.dump(BGGstew, fileObject)
fileObject.close()

fileObject = open("Objects/20181118_sample_pickle", "wb")
pickle.dump(sample, fileObject)
fileObject.close()



# Having collected all of the desired data available about the individual boardgames, we now need to make one 
# final API call to fill in the missing information about user that generated each rating. The user API returns 
# a minimal number of fields, so we will directly store the user's registration year and year of last login in sample.

# Retrieved 20181119.

# add additional columns to sample for the new fields
#sample["Registration Year"] = np.nan
#sample["Last Login"] = np.nan

# iterate over the rows of the sample dataframe
for index, row in sample.iloc.iterrows():
  # call the API, inserting game number and the page number 
  address = APIuser.format(row["Username"])
  requested_data = requests.get(address)
  
  # convert the returned XML into a soup object
  soup = BeautifulSoup(requested_data.text, "xml")
  
  # pull the registration year and last login year out of the soup object into sample
  sample.loc[index, "Registration Year"] = soup.find("yearregistered")["value"]
  sample.loc[index, "Last Login"] = soup.find("lastlogin")["value"]
  
  # pause briefly to avoid overtaxing the API
  time.sleep(5)

# "pickle" completed sample dataframe for future retrieval
fileObject = open("Objects/20181120_sample_Pickle", "wb")
pickle.dump(sample, fileObject)
fileObject.close()

