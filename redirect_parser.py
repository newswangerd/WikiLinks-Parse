#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''

The purpose of this file is to parse an XML dump of wikipedia and extract all of the titles
of the pages that have the redirect tag. Once these titles are gathered, they are matched
with the article that the page redirects to and saved in a MySQL database.

By: David Newswanger and Robert Hosking

Credit: http://effbot.org/zone/element-iterparse.htm for description of how to use
iterparse for parsing large XML files.

'''

import xml.etree.cElementTree as et
import time
import mysql.connector
from mysql.connector.errors import IntegrityError

def main():
    source = '/media/david/Storage/enwiki-20151102-pages-articles.xml'
    #source = '100000_lines.xml'
    print "Establishing Connection to Database"
    conn = mysql.connector.connect(user="test", password="test", host="localhost", database="wiki_links")
    cursor = conn.cursor()
    
    # Loads all of the articles in the database into a dictionary for fast lookups
    articles = get_db_titles(conn)
    
    # get an iterable
    context = et.iterparse(source, events=("start", "end"))
    
    # turn it into an iterator
    context = iter(context)
    
    # get the root element
    event, root = context.next()
    start_time = time.time()
    counter = 1
    print "Initializing Scan"
    missing_count = 0
    missing = []
    
    # Loops through every tag in the XML file
    for event, elem in context:
        
        # If the tage is a "page tag, parse it
        if event == "end" and elem.tag == "{http://www.mediawiki.org/xml/export-0.10/}page":
            try: 
                # Checks to see if the page tag contains a redirect tag
                redirect = elem.find('{http://www.mediawiki.org/xml/export-0.10/}redirect')
                title = elem.find('{http://www.mediawiki.org/xml/export-0.10/}title').text
                if redirect != None:
                    destination = redirect.attrib['title']
                    
                    # If the page contains a redirect tag, the program tries to find it in the articles dictionary
                    try:
                        id = articles[destination]
                    except KeyError:
                        missing_count += 1
                        missing.append((title, destination))
                        id = 0
                        
                    # Attempts to insert the data into the database
                    query = "INSERT INTO redirect(title, article_id) VALUES(%s, %s)"
                    try :
                        cursor.execute(query, (title, id))
                        conn.commit()
                    
                    # If there are duplicates then this will fail
                    except IntegrityError:
                        print "Duplicate: " + elem.find('{http://www.mediawiki.org/xml/export-0.10/}title').text
                    
                    # progress is update after every 1000 articles
                    if counter % 1000 == 0: 
                        print "Scanned %s articles." % counter
                        print str((time.time() - start_time) / 60) + " minutes elapsed."
                    
                    counter += 1
                    root.clear() # Keeps the parser from eating up all our memory
            
            # Prevents crashing when elem.find returns None
            except AttributeError:
                pass
        
    print "failures: "
    print missing_count
    print missing
    cursor.close()
    conn.close()
    
def get_db_titles(conn):
    '''
    
    Pre: Receives a connection object to a database which contains an articles table that has a
         title and ID field.
    
    Post: Returns a dictionary with the title as the key and id as value
    
    '''
    cursor = conn.cursor()

    query = ("SELECT * FROM articles")
    cursor.execute(query)
    current = time.time()
    articles = {}
    for (id, title) in cursor:
        articles[title.decode('utf8')] = id
    
    cursor.close()
    
    return articles
main()