# discourse-reader
Data pipeline to access and store data from Discourse forums

#### Design decisions

- Currently storing dynamic data in the topics table, instead of relying on joins with posts and other tables to generate dynamic counts
- Pulling down entire scrape of raw API each time, would be more efficient to find a way to get all new information from a given date.
