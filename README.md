# discourse-reader
Data pipeline to access and store data from Discourse forums

### Design decisions

- Currently storing dynamic data in the topics table, instead of relying on joins with posts and other tables to generate dynamic counts
- Pulling down entire scrape of raw API each time, would be more efficient to find a way to get all new information from a given date.

### Raw Data Structure

##### 1. Users
      - Single JSON file containing all information returned about users in the discourse API
      - List of dictionaries (no edits from API)

##### 2. Raw Categories
      - Single JSON file containing all general information about each category
      - List of dictionaries (no edits from API)

##### 2. Individual Categories
      - Individual JSON file for each category returned by the API
      - Singular dictionary for each file
      - Posts and Topics fields are inserted into the dicationary in addition to raw information from API
