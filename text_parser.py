'''

The purpose of this file is to pase all of the articles in a wikipedia XML dump file,
extract all of the titles of all of the internal links in each page, and save them
to a database.

By: David Newswanger and Robert Hosking

Credit: http://effbot.org/zone/element-iterparse.htm for description of how to use
iterparse for parsing large XML files.

'''
import re
import xml.etree.cElementTree as et
import time
import mysql.connector

def main():
    print "Establishing Connection to Database"
    conn = mysql.connector.connect(user="test", password="test", host="localhost", database="wiki_links")
    cursor = conn.cursor()
    
    # Checks to see if the database is populated. If it is, it takes the latest article to be parsed,
    # deletes the links in it and starts from that article. This allows the scan to be resumed from wher
    # you left off if it fails.
    cursor.execute("SELECT MAX(id) FROM links")
    current_title = ""
    restore = False
    current = cursor.fetchone()
    print current
    if current[0] != None:
        print "Restoring from Previous Progress"
        cursor.execute("SELECT title_id FROM links WHERE id = %s", current)
        current_title = cursor.fetchone()
        print current_title
        cursor.execute("DELETE FROM links WHERE title_id = %s", current_title)
        conn.commit()
        cursor.execute("SELECT title FROM articles WHERE id = %s", current_title)
        current_title = cursor.fetchone()
        current_title = current_title[0].decode('utf8')
        print current_title
        restore = True

    
    source = '/media/david/Storage/enwiki-20151102-pages-articles.xml'    
    context = et.iterparse(source, events=("start", "end"))
    
    # Loads all the data from the articles and redirect table in the databse.
    print "Loading articles from database"
    articles, redirect = get_db_titles(conn)
    
    context = iter(context)
    
    event, root = context.next()
    counter = 1
    print "Initializing Scan"
    start_time = time.time()
    
    for event, elem in context:
        
        # If the tag is a page tag
        if event == "end" and elem.tag == "{http://www.mediawiki.org/xml/export-0.10/}page":
            try: #Avoids some cases where tags are empty
                if elem.find('{http://www.mediawiki.org/xml/export-0.10/}redirect') == None:
                    title = elem.find('{http://www.mediawiki.org/xml/export-0.10/}title').text
                    
                    # If you are restoring to a previous location, skip the text parsing.
                    if restore:
                        if title == current_title: 
                            print "Caught up with latest record"
                            restore = False
                    else:
                        
                        # Find the text tag, which is inside the revision tag
                        text = elem.find('{http://www.mediawiki.org/xml/export-0.10/}revision').find('{http://www.mediawiki.org/xml/export-0.10/}text').text
                        
                        # Creates a list with a set of tuples that contain the id of the current page
                        # and the id for each page that the current page links to.
                        links = get_link_pairs(articles, title, redirect, text)
                        cursor.executemany("INSERT INTO links (title_id, link_id) VALUES (%s, %s)", links)
                        conn.commit()
                    counter += 1
                    
                    # Updates the user ever 1000 articles.
                    if counter % 1000 == 0: 
                        print "Scanned %s articles. %s percent complete" % (counter, str(counter / 85000))
                        print str((time.time() - start_time) / 60) + " minutes elapsed."
                    root.clear()

            # Prevents crashing when elem.find returns None
            except AttributeError:
                pass

        
    
    cursor.close()
    conn.close()

def get_link_pairs(articles, title, redirect, text):
    '''
    
    Pre: dictionairy with titles, and title ids, redirects and title ids, current page title
         and text to parse.
         
    Post: Returns a list of tuples where the first element of the tuple contains the id of the
          current page and the second element contains the id for the page that the link points
          to.
    
    '''
    linker = []
    try:
        #\[\[(.*?)\]\]
        # Bad Regex: \[\[(?:[^|\]]*\|)?([^\]]+)\]\]
        
        # Matches all of the patterns in the text that contain [[ and ]], which are used to create links
        # in wikipedia articles
        links = re.findall(r'\[\[(.*?)\]\]', text)
        for link in links:
            link = get_link(link)
            
            # See if the link exists in the articles dictionary. If it doesn't, check to see if it
            # is in the redirects dictionary. If not, ignore it because it doesn't link to anything
            # in our databse. 
            try:
                if (articles[title], articles[link]) not in linker:
                    linker.append((articles[title], articles[link]))
            except KeyError: 
                try:
                    if (articles[title], redirect[link]) not in linker:
                        linker.append((articles[title], redirect[link]))
                except KeyError:
                    pass
    
    # Prevents the script from crashing when an invalid string of text is passed.
    except TypeError:
        pass
    
    return linker


def get_link(link):
    
    '''
    
    Pre: Receives a wikipedia formatted link without the [[ ]] on either end.
    Post: Returns the title of the page that the link links to or false if it is invalid.
    
    Examples:
    City if London#Metro|London Underground --> City of London
    help:Link Formatting|Learn to format links --> help:Link Formatting
    #Subheading --> False
    Potatoes --> Potatoes
    
    '''
    
    namespaces = ['wikipedia', 'help', 'category', 'mediawiki']
    
    # Returns false if invalid link is passed.
    if link == None: return False
    if link == "": return False
    
    # Links that start with # point to the current article and are thrown out.
    if link[0] == "#": return False
    
    # Splits the link up into it's namespaces, which are separated by :
    link = link.split(":")
    namespace = ""
    if len(link) > 1:
        index = 0
        
        # If the namespace starts with :, the first item in the list is "" and we ignore it
        if link[0] == "":
            index = 1
        # If namespace is not in accepted namespaces, return false
        
        # If the article has a namespace and it's not in the accepted list of namespaces, throw the link out
        if link[index].lower() not in namespaces:
            return False
        else:
            # Extract the namespace from the string
            try:
                namespace = link[index] + ":"
                link = link[index + 1]
            except IndexError: return False
    else: link = link[0]
    
    # Pick everything to the left of | and #
    link = link.split("|")[0]
    link = link.split("#")[0]
    
    # Return link and namespace
    return namespace + link

def get_db_titles(conn):
    '''
    
    Pre: Receives a connection object to a database which contains an articles and redirect table.
    
    Post: Returns a two dictionaries with the title of the article or redirect as the key and page id as value
    
    '''
    cursor = conn.cursor()

    query = ("SELECT * FROM articles")
    cursor.execute(query)
    articles = {}
    for (id, title) in cursor:
        articles[title.decode('utf8')] = id
        
    query = ("SELECT title, article_id FROM redirect")
    cursor.execute(query)
    redirect = {}
    for (title, article_id) in cursor:
        articles[title.decode('utf8')] = article_id
    
    cursor.close()
    
    return (articles, redirect)

main()