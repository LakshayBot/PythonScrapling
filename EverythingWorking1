from scrapling.fetchers import StealthyFetcher
from scrapling.parser import Selector
import json
import asyncio
import re
import psycopg2
from psycopg2 import pool
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from datetime import datetime
import signal
import sys

async def get_single_video_url(url, semaphore, db_conn=None):
    """Process a single video URL with StealthyFetcher using semaphore for concurrency control"""
    async with semaphore:  # Use semaphore to limit concurrent processing
        # Extract post ID for logging/filters
        m = re.search(r"/post/([a-f0-9]+)\.html", url)
        post_id = m.group(1) if m else None
        print(f"Processing {post_id or url} with StealthyFetcher...")

        captured = []

        # Define page action for media capture
        async def page_action(page):
            # Listen to network responses and capture media-like URLs
            def on_response(response):
                u = response.url
                if any(ext in u.lower() for ext in ('.vid', '.m3u8', '.mp4', '/videoplayback')):
                    if (post_id is None) or (post_id in u):
                        captured.append(u)
                        print(f"Captured media URL: {u}")

            page.on('response', on_response)

            # Wait for page to fully load after Cloudflare
            try:
                await page.wait_for_load_state('domcontentloaded', timeout=30000)
                print(f"Page loaded for {post_id}")
            except Exception as e:
                print(f"Error waiting for page load: {e}")

            # Try to find and interact with player elements
            selectors = [
                '#player_el', 'video', '.video-player', '[data-video]', '.player',
                '[class*="play"]', '[id*="video"]', '[id*="player"]'
            ]
            for sel in selectors:
                try:
                    el = await page.query_selector(sel)
                    if el:
                        print(f"Found player element: {sel}")
                        await el.click()
                        await page.wait_for_timeout(2000)  # Wait longer for video to load
                        if captured:
                            print(f"Captured video URL after clicking {sel}")
                            break
                except Exception as e:
                    print(f"Error with selector {sel}: {e}")
                    continue

            # Longer grace period for late network requests
            if not captured:
                print(f"No video captured yet, waiting longer for {post_id}...")
                await page.wait_for_timeout(3000)
            
            return page

        try:
            # Use StealthyFetcher.async_fetch as class method (v0.3+ pattern)
            response = await StealthyFetcher.async_fetch(
                url,
                headless=True,
                solve_cloudflare=True,  # Solve Cloudflare on video pages too
                network_idle=True,      # Wait for network idle
                page_action=page_action,
                timeout=90000  # Increased timeout for Cloudflare + video loading
            )
            
            video_url = captured[0] if captured else ''
            
            if not video_url:
                print(f"WARNING: No video URL captured for {url}")
            
            # Update the database immediately if connection is provided
            if db_conn and video_url:
                try:
                    with db_conn.cursor() as cursor:
                        cursor.execute(
                            "UPDATE videos SET video_source_url = %s WHERE url = %s",
                            (video_url, url)
                        )
                        db_conn.commit()
                        print(f"Updated video URL in database for post: {url}")
                except Exception as e:
                    print(f"Error updating video URL in database: {e}")
                    if db_conn:
                        db_conn.rollback()

            return url, video_url
            
        except Exception as e:
            print(f"Error processing {url}: {e}")
            return url, ''

async def get_video_urls_async(post_urls, max_concurrent=3, db_conn=None):
    """Extract video source URLs using StealthyFetcher with parallel processing"""
    video_urls = {}
    
    # Create semaphore to limit concurrent browser tabs
    semaphore = asyncio.Semaphore(max_concurrent)
    
    # Create tasks for all URLs
    tasks = []
    for url in post_urls:
        tasks.append(get_single_video_url(url, semaphore, db_conn))
    
    # Execute all tasks concurrently
    print(f"Processing {len(post_urls)} video URLs with {max_concurrent} concurrent browsers...")
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Process results
    for i, result in enumerate(results):
        url = post_urls[i]
        if isinstance(result, Exception):
            print(f"Error processing {url}: {result}")
            video_urls[url] = ''
        else:
            url, video_url = result
            video_urls[url] = video_url
    
    return video_urls

def get_video_urls(post_urls, max_concurrent=3, db_conn=None):
    """Non-async wrapper for get_video_urls_async to maintain compatibility"""
    return asyncio.run(get_video_urls_async(post_urls, max_concurrent, db_conn))
    
def insert_post_to_database(post_data, conn):
    """Insert a single post into PostgreSQL database
    
    Args:
        post_data: Dictionary containing post information
        conn: PostgreSQL connection object
    """
    try:
        with conn.cursor() as cursor:
            # Convert post_data to JSON string
            json_data = json.dumps([post_data])
            
            # Call the PostgreSQL function to insert the data
            cursor.execute("SELECT insert_videos_from_json(%s::jsonb);", (json_data,))
            
            # Commit the changes
            conn.commit()
            print(f"Inserted post into database: {post_data.get('title', '')[:30]}...")
    except Exception as e:
        print(f"Error inserting into database: {e}")
        # Rollback the transaction if an error occurred
        if conn:
            conn.rollback()


def parse_posts(html_content):
    """Parse post data from HTML content with enhanced selector coverage"""
    parsed_page = Selector(html_content)
    
    # Try multiple possible selectors for posts
    posts = parsed_page.css('.post_el_small .post_control')
    
    data = []
    for post in posts:
        try:
            # Try multiple selectors for post title/link
            a_tag = post.css_first('a.post_time') or post.css_first('a[href*="post"]') or post.css_first('a')
            if not a_tag:
                continue
                
            title = a_tag.attrib.get('title', '') or a_tag.text or ''
            post_url = a_tag.attrib.get('href', '')
            
            if post_url:
                # Ensure URL is absolute
                if not post_url.startswith('http'):
                    full_post_url = "https://sxyprn.com" + post_url
                else:
                    full_post_url = post_url
            else:
                full_post_url = ''
            
            # Try multiple selectors for time
            time_span = post.css_first('.post_control_time span') or post.css_first('.time') or post.css_first('.date')
            post_control_time = time_span.text.strip() if time_span else ''
            
            data.append({
                'title': title,
                'post_url': full_post_url,
                'post_control_time': post_control_time,
                'video_url': ''
            })
            
            # Log successful parse for debugging
            print(f"Parsed post: {title[:30]}{'...' if len(title) > 30 else ''}")
            
        except Exception as e:
            print(f"Error parsing post: {e}")
            continue
    
    print(f"Successfully parsed {len(data)} posts")
    return data

async def scrape_single_channel(channel_id, channel_name, channel_url, last_post_urls_array, db_conn_pool):
    """
    Scrape a single channel with incremental scraping optimization.
    Only fetches new posts since last check by comparing against stored post URLs.
    
    Args:
        channel_id: Database ID of the channel
        channel_name: Name of the channel for logging
        channel_url: Base URL of the channel
        last_post_urls_array: List of previously seen post URLs (for deduplication)
        db_conn_pool: Database connection pool
    
    Returns:
        int: Count of new videos found and processed
    """
    start_time = datetime.now()
    print(f"\n{'='*80}")
    print(f"[{start_time.strftime('%Y-%m-%d %H:%M:%S')}] Starting scrape: {channel_name}")
    print(f"{'='*80}")
    
    db_conn = None
    new_posts = []
    all_scraped_posts = []
    
    try:
        # Get connection from pool
        db_conn = db_conn_pool.getconn()
        
        # Convert last_post_urls_array to set for faster lookup
        known_urls = set(last_post_urls_array) if last_post_urls_array else set()
        
        found_known_post = False
        
        # Fetch main page first
        print(f"\n--- Fetching main page for {channel_name} ---")
        
        try:
            await asyncio.sleep(2.0)
            
            # Use StealthyFetcher.async_fetch as class method (v0.3+ pattern)
            main_page = await StealthyFetcher.async_fetch(
                channel_url,
                headless=True,
                solve_cloudflare=True,
                network_idle=True,
                timeout=90000,
                wait_selector='.post_el_small',
                wait_selector_state='attached'
            )
            
            print(f"✓ Successfully fetched main page")
            
            # Parse the HTML to extract pagination links
            from scrapling.parser import Selector
            parsed_main = Selector(main_page.body)
            
            # Extract actual pagination links from the page
            pagination_links = parsed_main.css('#center_control a')
            page_urls = [channel_url]  # Start with main page
            
            for link in pagination_links:
                href = link.attrib.get('href')
                if href and href not in page_urls:
                    full_url = "https://sxyprn.com" + href if not href.startswith('http') else href
                    page_urls.append(full_url)
                    print(f"Found pagination URL: {full_url}")
            
            # Limit pagination pages (safety)
            MAX_PAGES = min(len(page_urls), 20)
            page_urls = page_urls[:MAX_PAGES]
            
            print(f"Will scrape {len(page_urls)} page(s) for {channel_name}")
            
            # Now scrape each page sequentially
            for page_idx, page_url in enumerate(page_urls, 1):
                print(f"\n--- Processing page {page_idx}/{len(page_urls)} ---")
                
                try:
                    # Fetch page (skip main page since we already have it)
                    if page_idx == 1:
                        page = main_page
                    else:
                        await asyncio.sleep(2.0)
                        
                        # Use StealthyFetcher.async_fetch as class method (v0.3+ pattern)
                        page = await StealthyFetcher.async_fetch(
                            page_url,
                            headless=True,
                            solve_cloudflare=True,
                            network_idle=True,
                            timeout=90000,
                            wait_selector='.post_el_small',
                            wait_selector_state='attached'
                        )
                        print(f"✓ Successfully fetched page {page_idx}")
                    
                    # Parse posts from page
                    posts = parse_posts(page.body)
                    
                    if not posts:
                        print(f"No posts found on page {page_idx}, stopping")
                        break
                    
                    # Check each post
                    found_new_posts = False
                    for post in posts:
                        post_url = post['post_url']
                        
                        # ALWAYS add to all_scraped_posts (for updating last_post_urls_array)
                        post_copy = post.copy()
                        post_copy['channel_name'] = channel_name
                        all_scraped_posts.append(post_copy)
                        
                        if post_url in known_urls:
                            print(f"✓ Found known post: {post['title'][:50]}... - Stopping pagination")
                            found_known_post = True
                            break
                        else:
                            # New post - add to new_posts list
                            new_posts.append(post_copy)
                            found_new_posts = True
                    
                    # Stop if found known post
                    if found_known_post:
                        break
                    
                    # Stop if no new posts found
                    if not found_new_posts and page_idx > 1:
                        print(f"⚠ Page {page_idx} had no new posts - stopping")
                        break
                        
                except Exception as e:
                    print(f"✗ Error processing page {page_idx}: {e}")
                    continue
                    
        except Exception as e:
            print(f"✗ Error fetching main page: {e}")
            import traceback
            traceback.print_exc()
        
        print(f"\n{'='*60}")
        print(f"Found {len(new_posts)} new post(s) for {channel_name}")
        print(f"{'='*60}")
        
        # Insert new posts to database
        if new_posts:
            for post in new_posts:
                insert_post_to_database(post, db_conn)
            print(f"✓ Inserted {len(new_posts)} new posts to database")
        
        # Process video URLs for new posts only
        new_post_urls = [p['post_url'] for p in new_posts if p['post_url']]
        
        if new_post_urls:
            print(f"\nProcessing {len(new_post_urls)} video URLs...")
            
            # Process videos with concurrency limit (reduced from 5 to 3 to prevent memory leaks)
            video_urls = await get_video_urls_async(new_post_urls, max_concurrent=3, db_conn=db_conn)
            
            # Update posts with video URLs
            for post in new_posts:
                if post['post_url'] in video_urls:
                    post['video_url'] = video_urls.get(post['post_url'], '')
        
        # Update channel metadata in database
        # Store the newest 10 post URLs for next run
        newest_urls = [p['post_url'] for p in all_scraped_posts[:10] if p['post_url']]
        
        # CRITICAL: If no new posts were scraped but we have known URLs, keep them
        # This prevents re-scraping on next run when all posts are already in database
        if not newest_urls and last_post_urls_array:
            print(f"⚠ No new posts scraped - keeping existing {len(last_post_urls_array)} known URLs")
            newest_urls = list(last_post_urls_array)[:10]
        
        with db_conn.cursor() as cursor:
            cursor.execute("""
                UPDATE channels 
                SET last_checked = NOW(),
                    last_post_url = %s,
                    last_post_urls_array = %s
                WHERE id = %s
            """, (
                newest_urls[0] if newest_urls else None,
                newest_urls if newest_urls else [],
                channel_id
            ))
            db_conn.commit()
        
        print(f"✓ Updated channel metadata (stored {len(newest_urls)} post URLs for next run)")
        
        # Calculate duration
        duration = (datetime.now() - start_time).total_seconds()
        print(f"\n{'='*80}")
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Completed: {channel_name}")
        print(f"New videos: {len(new_posts)} | Duration: {duration:.1f}s")
        print(f"{'='*80}\n")
        
        return len(new_posts)
        
    except Exception as e:
        print(f"\n✗ ERROR in scrape_single_channel for {channel_name}: {e}")
        import traceback
        traceback.print_exc()
        return 0
        
    finally:
        # Return connection to pool
        if db_conn:
            db_conn_pool.putconn(db_conn)
            print(f"✓ Returned database connection to pool")


try:
    # PostgreSQL connection parameters
    pg_params = {
        'dbname': 'MonitoringDatabase',
        'user': 'postgres',
        'password': 'postgres',
        'host': '192.168.1.3',
        'port': '5432'
    }
    
    # Create connection pool for 24/7 operation
    print("Creating database connection pool...")
    db_pool = psycopg2.pool.SimpleConnectionPool(
        minconn=2,
        maxconn=10,
        **pg_params
    )
    print("✓ Database connection pool created (2-10 connections)")
    
    # Create scheduler
    scheduler = AsyncIOScheduler()
    
    # Track which channels are currently scraping (to prevent overlaps)
    currently_scraping = set()
    
    # Graceful shutdown handler
    def shutdown_handler(signum, frame):
        print(f"\n{'='*80}")
        print("Received shutdown signal. Shutting down gracefully...")
        print(f"{'='*80}\n")
        scheduler.shutdown(wait=True)
        db_pool.closeall()
        print("✓ Shutdown complete")
        sys.exit(0)
    
    # Register signal handlers
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)
    
    # Wrapper function for scheduled jobs with error handling
    async def scheduled_scrape_wrapper(channel_id, channel_name, channel_url, check_interval_minutes):
        """Wrapper to handle errors and prevent scheduler crashes"""
        # Check if this channel is already being scraped
        if channel_id in currently_scraping:
            print(f"⏭️  [{channel_name}] Skipping - scrape already in progress")
            return
        
        try:
            # Mark channel as currently scraping
            currently_scraping.add(channel_id)
            print(f"🔄 [{channel_name}] Starting scheduled scrape...")
            
            # Get latest channel data from DB (in case last_post_urls_array was updated)
            conn = db_pool.getconn()
            try:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT last_post_urls_array 
                        FROM channels 
                        WHERE id = %s
                    """, (channel_id,))
                    result = cursor.fetchone()
                    last_post_urls_array = result[0] if result and result[0] else []
            finally:
                db_pool.putconn(conn)
            
            # Run the scrape
            new_count = await scrape_single_channel(
                channel_id,
                channel_name,
                channel_url,
                last_post_urls_array,
                db_pool
            )
            
            print(f"✓ [{channel_name}] Scheduled scrape completed: {new_count} new videos")
            
        except Exception as e:
            print(f"✗ ERROR in scheduled scrape for {channel_name}: {e}")
            import traceback
            traceback.print_exc()
            # Don't re-raise - let other channels continue
        
        finally:
            # Always remove from currently_scraping set
            currently_scraping.discard(channel_id)
    
    # Fetch active channels and schedule jobs
    print("\n" + "="*80)
    print("Initializing scheduler with active channels...")
    print("="*80 + "\n")
    
    conn = db_pool.getconn()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT id, name, url, check_interval_minutes, last_post_urls_array, last_checked
                FROM public.channels 
                WHERE is_active = true
                ORDER BY name
            """)
            channels = cursor.fetchall()
    finally:
        db_pool.putconn(conn)
    
    if not channels:
        print("No active channels found in database!")
        sys.exit(0)
    
    print(f"Found {len(channels)} active channel(s):\n")
    
    # Store channel data for initial scrape
    channels_data = []
    
    # Schedule each channel as separate job
    for channel_id, channel_name, channel_url, check_interval_minutes, last_post_urls_array, last_checked in channels:
        interval = check_interval_minutes or 60  # Default to 60 minutes if not set
        
        print(f"  📅 {channel_name}")
        print(f"     URL: {channel_url}")
        print(f"     Interval: {interval} minutes")
        print(f"     Known posts: {len(last_post_urls_array) if last_post_urls_array else 0}")
        print(f"     Last checked: {last_checked or 'Never'}")
        print()
        
        # Store for initial scrape
        channels_data.append((channel_id, channel_name, channel_url, interval, last_post_urls_array, last_checked))
        
        # Add job to scheduler (will run on interval after first manual run)
        scheduler.add_job(
            scheduled_scrape_wrapper,
            trigger=IntervalTrigger(minutes=interval),
            args=[channel_id, channel_name, channel_url, interval],
            id=f"channel_{channel_id}",
            name=f"Scrape: {channel_name}",
            max_instances=1,  # Prevent overlapping runs
            replace_existing=True
        )
    
    print(f"{'='*80}")
    print(f"✓ Scheduled {len(channels)} channel(s) successfully")
    print(f"{'='*80}\n")
    
    # Start scheduler within event loop and keep it running
    async def run_scheduler():
        # Start the scheduler
        scheduler.start()
        
        print("🚀 Checking which channels need initial scrape...\n")
        
        # Only run initial scrape for channels that haven't been checked recently
        # This prevents re-scraping everything on container restart
        from datetime import timedelta, timezone
        
        channels_to_scrape = []
        for channel_id, channel_name, channel_url, interval, last_post_urls_array, last_checked in channels_data:
            # Skip initial scrape if checked within the last interval period
            should_skip = False
            if last_checked:
                # Get current time in UTC
                now = datetime.now(timezone.utc)
                
                # If last_checked is naive (no timezone), assume it's UTC
                if last_checked.tzinfo is None:
                    last_checked = last_checked.replace(tzinfo=timezone.utc)
                
                time_since_check = now - last_checked
                mins_ago = int(time_since_check.total_seconds() / 60)
                
                # Debug: show the comparison
                print(f"   {channel_name}: last_checked={last_checked}, now={now}, diff={mins_ago} mins")
                
                # Only skip if time difference is positive and within interval
                # (negative means database time is in the future - timezone issue, so scrape it)
                if mins_ago >= 0 and time_since_check < timedelta(minutes=interval):
                    print(f"⏭️  Skipping {channel_name} - already scraped {mins_ago} minutes ago")
                    should_skip = True
                elif mins_ago < 0:
                    print(f"⚠️  {channel_name}: Database time is in the future (timezone mismatch) - will scrape")
            
            if not should_skip:
                channels_to_scrape.append((channel_id, channel_name, channel_url, interval, last_post_urls_array))
        
        if channels_to_scrape:
            print(f"\n{'='*80}")
            print(f"INITIAL SCRAPE - Running {len(channels_to_scrape)} channel(s)")
            print(f"{'='*80}\n")
            
            # Run initial scrape for channels that need it
            for channel_id, channel_name, channel_url, interval, last_post_urls_array in channels_to_scrape:
                print(f"Starting initial scrape for: {channel_name}")
                await scheduled_scrape_wrapper(channel_id, channel_name, channel_url, interval)
            
            print(f"\n{'='*80}")
            print("✓ Initial scrape completed")
            print(f"{'='*80}\n")
        else:
            print(f"\n{'='*80}")
            print("✓ All channels recently scraped - skipping initial scrape")
            print(f"{'='*80}\n")
        
        print(f"{'='*80}")
        print("SCHEDULER RUNNING - 24/7 Operation Mode")
        print(f"{'='*80}")
        print("Channels will be scraped automatically based on their intervals.")
        print("Press Ctrl+C to stop the scheduler gracefully.\n")
        
        # Keep the event loop running forever
        try:
            while True:
                await asyncio.sleep(3600)  # Sleep for 1 hour intervals
        except (KeyboardInterrupt, SystemExit):
            pass
    
    # Run the scheduler
    try:
        asyncio.run(run_scheduler())
    except (KeyboardInterrupt, SystemExit):
        print("\n⚠ Shutdown initiated by user...")
    except Exception as e:
        print(f"\n✗ Scheduler crashed: {e}")
        import traceback
        traceback.print_exc()
        raise  # Re-raise to ensure proper cleanup

except Exception as e:
    print(f"✗ FATAL ERROR: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)  # Exit with error code

finally:
    # Cleanup - ALWAYS runs even on crash
    print("\n" + "="*80)
    print("CLEANUP - Closing connections and shutting down...")
    print("="*80)
    
    # Stop scheduler if running
    if 'scheduler' in locals() and scheduler.running:
        try:
            print("Stopping scheduler...")
            scheduler.shutdown(wait=False)
            print("✓ Scheduler stopped")
        except Exception as e:
            print(f"⚠ Error stopping scheduler: {e}")
    
    # Close database connection pool
    if 'db_pool' in locals() and db_pool:
        try:
            print("Closing database connection pool...")
            db_pool.closeall()
            print("✓ Database connections closed")
        except Exception as e:
            print(f"⚠ Error closing database pool: {e}")
    
    print("✓ Cleanup complete")
    print("="*80 + "\n")
