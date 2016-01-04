'''

Purpose: Given a wikipedia xml dump file, extracts all the names of the articles
and saves them to a database

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
    
    print "Establishing Connection to Database"
    conn = mysql.connector.connect(user="test", password="test", host="localhost", database="wiki_links")
    cursor = conn.cursor()
    
    # get an iterable
    context = et.iterparse(source, events=("start", "end"))
    
    # turn it into an iterator
    context = iter(context)
    
    # get the root element
    event, root = context.next()
    counter = 1
    print "Initializing Scan"
    start_time = time.time()
    
    for event, elem in context:
        
        # Checks the current tag to see if it is a page tag
        if event == "end" and elem.tag == "{http://www.mediawiki.org/xml/export-0.10/}page":
            try:
                
                # If the page tag contains a redirect tag, skip it.
                if elem.find('{http://www.mediawiki.org/xml/export-0.10/}redirect') == None:
                    query = "INSERT INTO articles(title) VALUES(%s)"
                    
                    # Attempts to insert the page into the database. If it is a duplicate, skip it.
                    try :
                        cursor.execute(query, (elem.find('{http://www.mediawiki.org/xml/export-0.10/}title').text,))
                        conn.commit()
                    except IntegrityError:
                        print "Duplicate: " + elem.find('{http://www.mediawiki.org/xml/export-0.10/}title').text
                    
                    # progress is update after every 1000 articles
                    if counter % 1000 == 0: 
                        print "Scanned %s articles." % counter
                        print str((time.time() - start_time) / 60) + " minutes elapsed."
                    root.clear()

            # Prevents crashing when elem.find returns None
            except AttributeError:
                pass
        
    
    cursor.close()
    conn.close()
main()