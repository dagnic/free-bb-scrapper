import re
import requests
import sqlite3
import os
from datetime import datetime

from urllib.parse import urljoin

from bs4 import BeautifulSoup

# To be filled
BASE_URL = ""
USERNAME = ""
PASSWORD = ""

LOGIN_PAGE = f"{BASE_URL}/login"
LOGIN_POST = f"{BASE_URL}/login_check"


DB_NAME = "forum_data.db"

session = requests.Session()

def init_database():
    """Initialize SQLite database with required tables"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    # Create forums table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS forums (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_name TEXT,
            title TEXT,
            description TEXT,
            url TEXT UNIQUE,
            subjects INTEGER,
            replies INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Create threads table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS threads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            forum_id INTEGER,
            title TEXT,
            url TEXT UNIQUE,
            author TEXT,
            replies INTEGER,
            views INTEGER,
            last_date TEXT,
            last_author TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (forum_id) REFERENCES forums (id)
        )
    ''')
    
    # Create messages table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            thread_id INTEGER,
            author TEXT,
            content TEXT,
            post_date TEXT,
            post_number INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (thread_id) REFERENCES threads (id)
        )
    ''')
    
    conn.commit()
    conn.close()
    print(f"Database {DB_NAME} initialized successfully!")

def save_forum_to_db(forum_data):
    """Save forum data to database and return forum ID"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    cursor.execute('''
        INSERT OR REPLACE INTO forums 
        (group_name, title, description, url, subjects, replies)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (
        forum_data["group"],
        forum_data["title"],
        forum_data["description"],
        forum_data["url"],
        forum_data["subjects"],
        forum_data["replies"]
    ))
    
    forum_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return forum_id

def save_thread_to_db(thread_data, forum_id):
    """Save thread data to database and return thread ID"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    cursor.execute('''
        INSERT OR REPLACE INTO threads 
        (forum_id, title, url, author, replies, views, last_date, last_author)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        forum_id,
        thread_data["title"],
        thread_data["url"],
        thread_data["author"],
        thread_data["replies"],
        thread_data["views"],
        thread_data["last_date"],
        thread_data["last_author"]
    ))
    
    thread_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return thread_id

def save_message_to_db(message_data, thread_id):
    """Save message data to database"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    cursor.execute('''
        INSERT OR IGNORE INTO messages 
        (thread_id, author, content, post_date, post_number)
        VALUES (?, ?, ?, ?, ?)
    ''', (
        thread_id,
        message_data["author"],
        message_data["content"],
        message_data["post_date"],
        message_data["post_number"]
    ))
    
    conn.commit()
    conn.close()

def login():
    # Get login page to retrieve CSRF token
    r = session.get(LOGIN_PAGE)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    csrf_token = soup.find("input", {"name": "_csrf_token"})["value"]

    # Build payload exactly like the form
    payload = {
        "_username": USERNAME,
        "_password": PASSWORD,
        "_remember_me": "on",      # optional
        "_csrf_token": csrf_token,
        "_submit": "Connexion",    # matches the submit button value
    }
    
    print("payload", payload)

    # POST credentials
    r = session.post(LOGIN_POST, data=payload)
    r.raise_for_status()

    # Check login success (your username should appear in HTML somewhere)
    if "Mon profil" not in r.text:
        raise RuntimeError("Login failed, check credentials")
    print("Logged in successfully!")


def get_forums():
    r = session.get(BASE_URL)   # after login
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    forums = []

    # Loop over each forum group
    for group in soup.select("div.containerGroup"):
        group_name = group.find("h4").get_text(strip=True)

        for forum_row in group.select("div.forum-row.catLink"):
            forum_link = forum_row.find("a", class_="categoryLink")
            if not forum_link:
                continue

            forum_url = BASE_URL + forum_link["href"]
            forum_title = forum_link.get_text(strip=True)
            forum_desc = forum_row.find("div", class_="forumdesc").get_text(strip=True)

            # Stats
            stats_col = forum_row.find("div", class_="col-md-1")
            subjects, replies = None, None
            if stats_col:
                stats_text = stats_col.get_text(" ", strip=True)
                # "111 sujets 1 699 réponses"
                parts = stats_text.replace("\xa0", " ").split()
                try:
                    subjects = int(parts[0])
                    replies = int(parts[2])
                except Exception:
                    pass

            forums.append({
                "group": group_name,
                "title": forum_title,
                "description": forum_desc,
                "url": forum_url,
                "subjects": subjects,
                "replies": replies,
            })

    return forums


def get_max_pages(soup):
    """Detects max number of pages from pagination block."""
    max_page = 1
    pag = soup.select_one("ul.pagination")
    if not pag:
        return max_page

    numbers = []
    for li in pag.find_all("li"):
        try:
            numbers.append(int(li.get_text(strip=True)))
        except ValueError:
            continue

    if numbers:
        max_page = max(numbers)
    return max_page

def get_threads(forum_url, max_pages=None):
    threads = []
    page = 1

    while True:
        # replace the page number in the URL if page > 1
        if page == 1:
            url = forum_url
        else:
            # Pattern: sujet-XXXXXX-XXXXXX-XXXXX-[page]-[title].html
            url = re.sub(r"(liste-\d+-\d+)-(\d+)-(.*)\.html$", f"\\1-{page}-\\3.html", forum_url)

        r = session.get(url)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        # detect total number of pages (once)
        if page == 1:
            total_pages = get_max_pages(soup)
            if max_pages is not None:
                total_pages = min(total_pages, max_pages)

        rows = soup.select("div.row.forum-row")
        if not rows:
            break

        for row in rows:
            link = row.select_one("div.tclcon a[href]")
            if not link:
                continue
            thread_url = urljoin(BASE_URL, link["href"])

            # Strip out #numN from URL if present
            thread_url = re.sub(r"#num\d+$", "", thread_url)

            # Force links to the first page.
            # Example: http://foo.free-bb.com/sujet-xxxxxx-xxxxxx-xxxxx-2-bar.html
            # becomes http://foo.free-bb.com/sujet-xxxxxx-xxxxxx-xxxxxx-1-bar.html
            # Pattern: sujet-XXXXXX-XXXXXX-XXXXX-[page]-[title].html
            thread_url = re.sub(r"(sujet-\d+-\d+-\d+)-(\d+)-(.*)\.html$", f"\\1-1-\\3.html", thread_url)

            title = link.get_text(strip=True)

            author_tag = row.select_one("div.tclcon a[itemprop='author']")
            author = author_tag.get_text(strip=True) if author_tag else None

            replies = views = None
            stats = row.select_one("div[itemprop='interactionStatistic']")
            if stats:
                strongs = stats.find_all("strong")
                if len(strongs) >= 2:
                    try:
                        replies = int(strongs[0].get_text(strip=True))
                        views = int(strongs[1].get_text(strip=True))
                    except ValueError:
                        pass

            lastpost = row.select_one("div.lastpostlink")
            last_date = last_author = None
            if lastpost:
                time_tag = lastpost.find("time")
                if time_tag:
                    last_date = time_tag.get_text(strip=True)
                user_tag = lastpost.select_one("span.byuser a")
                if user_tag:
                    last_author = user_tag.get_text(strip=True)

            threads.append({
                "title": title,
                "url": thread_url,
                "author": author,
                "replies": replies,
                "views": views,
                "last_date": last_date,
                "last_author": last_author,
            })

        if page >= total_pages:
            break
        page += 1

    return threads
    
def get_messages(thread_url, max_pages=None):
    """Scrape all messages from a thread"""
    messages = []
    page = 1
    total_post_count = 0  # Track total posts across all pages

    while True:
        # Build URL with page number
        if page == 1:
            url = thread_url
        else:
            # When multiple pages exist, it's like for list of threads:
            # Example: http://foo.free-bb.com/sujet-xxxxxx-xxxxxx-xxxxx-1,
            # http://foo.free-bb.com/sujet-xxxxxx-xxxxxx-xxxxx-2
            # Pattern: sujet-XXXXXX-XXXXXX-XXXXX-[page]-[title].html
            url = re.sub(r"(sujet-\d+-\d+-\d+)-(\d+)-(.*)\.html$", f"\\1-{page}-\\3.html", thread_url)

        try:
            r = session.get(url)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")

            # Detect total number of pages (once)
            if page == 1:
                total_pages = get_max_pages(soup)
                if max_pages is not None:
                    total_pages = min(total_pages, max_pages)

            # Find all message containers - look for the first post and regular posts
            # First post has class "firstpost topPost", regular posts have "topPost"
            first_post = soup.select_one("div.row.firstpost.topPost")
            regular_posts = soup.select("div.row.topPost:not(.firstpost)")
            
            all_posts = []
            if first_post:
                all_posts.append(first_post)
            all_posts.extend(regular_posts)
            
            if not all_posts:
                print(f"No posts found on page {page} of {thread_url}")
                break

            for post_num, post_container in enumerate(all_posts, start=total_post_count + 1):
                # Extract author from the author column
                author_elem = post_container.select_one("div.author a h4")
                if not author_elem:
                    author_elem = post_container.select_one("div.author h4")
                author = author_elem.get_text(strip=True) if author_elem else "Unknown"

                # Extract post date from the calendar div
                date_elem = post_container.select_one("div.calendar")
                post_date = "Unknown"
                if date_elem:
                    date_text = date_elem.get_text(strip=True)
                    # Remove the calendar icon text and get just the date
                    post_date = date_text.replace("", "").strip()

                # Extract message content from the next row's col-md-9
                content = ""
                next_row = post_container.find_next_sibling("div", class_="row")
                if next_row:
                    content_elem = next_row.select_one("div.col-md-9")
                    if content_elem:
                        # Check if it's a reply div (has class like "reply5950509")
                        reply_div = content_elem.select_one("div[class^='reply']")
                        if reply_div:
                            content = reply_div.get_text(strip=True)
                        else:
                            # For first post, content is directly in the col-md-9
                            # Remove any script tags and ads
                            for script in content_elem.select("script, ins"):
                                script.decompose()
                            content = content_elem.get_text(strip=True)

                # Skip if this is an ad row (contains google ads)
                if "google_ad_client" in content or "Liens sponsorisés" in content:
                    continue

                messages.append({
                    "author": author,
                    "content": content,
                    "post_date": post_date,
                    "post_number": post_num
                })

            # Update total post count for next page
            total_post_count += len(all_posts)

            if page >= total_pages:
                break
            page += 1

        except Exception as e:
            print(f"Error scraping page {page} of thread {thread_url}: {e}")
            break

    return messages
    
print(session)

if __name__ == "__main__":
    # Initialize database
    init_database()

    # Login first
    login()

    # Get all forums
    forums = get_forums()
    print(f"Found {len(forums)} forums")

    # Process each forum
    for forum in forums:
        print(f"\nProcessing forum: {forum['title']}")
        
        # Save forum to database
        forum_id = save_forum_to_db(forum)
        print(f"Saved forum with ID: {forum_id}")

        # Get threads from this forum
        threads = get_threads(forum["url"])
        print(f"Found {len(threads)} threads")

        # Process each thread
        for thread in threads:
            print(f"  Processing thread: {thread['title']}")
            
            # Save thread to database
            thread_id = save_thread_to_db(thread, forum_id)
            print(f"  Saved thread with ID: {thread_id}")

            # Get messages from this thread
            messages = get_messages(thread["url"])
            print(f"  Found {len(messages)} messages")
            
            # Save each message to database
            for message in messages:
                save_message_to_db(message, thread_id)
            
            print(f"  Saved {len(messages)} messages to database")

    print("\nScraping completed! All data saved to database.")