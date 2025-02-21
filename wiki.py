import json
from requests_sse import EventSource
import time
from collections import defaultdict
from datetime import datetime, timezone
import threading

# Wikipedia Event Stream API endpoint
WIKIPEDIA_STREAM_URL = 'https://stream.wikimedia.org/v2/stream/revision-create'

class WikipediaWatching:
    def __init__(self, report_minutes=1):
        self.report_minutes = report_minutes
        self.events = []
        self.timer = 0
        threading.Thread(target=self.fetch_wikipedia_events, daemon=True).start()

    # Function to fetch Wikipedia events in real-time
    def fetch_wikipedia_events(self):
        with EventSource(WIKIPEDIA_STREAM_URL) as stream:
            for event in stream:
                if event.type == 'message':
                    try:
                        change = json.loads(event.data)
                        if change['meta']['domain'] != 'canary':
                            self.events.append(change)
                    except ValueError:
                        continue

    # Function to generate reports based on the last n minutes of data
    def generate_reports(self, minutes=None):
        if minutes is None:
            minutes = self.report_minutes

        # Time threshold for events to consider
        current_utc_timestamp = datetime.now(timezone.utc).timestamp()
        threshold_time = current_utc_timestamp - (minutes * 60)
        filtered_events = [e for e in self.events if e['meta']['dt'] and datetime.strptime(e['meta']['dt'], '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc).timestamp() >= threshold_time]
        
        # Domains Report
        domain_page_count = defaultdict(set)
        for event in filtered_events:
            domain = event['meta']['domain']
            page_title = event['page_title']
            domain_page_count[domain].add(page_title)
        
        print(f"\nTotal number of Wikipedia Domains Updated: {len(domain_page_count)}")
        for domain, pages in sorted(domain_page_count.items(), key=lambda x: len(x[1]), reverse=True):
            print(f"{domain}: {len(pages)} pages updated")
        
        # Users Report
        user_edit_counts = {}
        for event in filtered_events:
            if event['meta']['domain'] == 'en.wikipedia.org' and not event['performer'].get('user_is_bot', False):
                user = event['performer']['user_text']
                edit_count = event['performer'].get('user_edit_count', 0)
                if user in user_edit_counts:
                    user_edit_counts[user] = max(user_edit_counts[user], edit_count)
                else:
                    user_edit_counts[user] = edit_count
        
        print("\nUsers who made changes to en.wikipedia.org")
        for user, edit_count in sorted(user_edit_counts.items(), key=lambda x: x[1], reverse=True):
            print(f"{user}: {edit_count}")
        print("\n")

    # Function to start generating reports periodically
    def start_reporting(self):
        while True:
            print("Fetching your reports for current time window...")
            time.sleep(60)
            self.timer = self.timer + 1
            last_time = max(0, self.timer - self.report_minutes)
            print(f"Reports for {last_time} to {self.timer}:")
            self.generate_reports()

if __name__ == '__main__':
    # Initialize with desired reporting window
    ww = WikipediaWatching(report_minutes=1)
    # ww = WikipediaWatching(report_minutes=5)
    ww.start_reporting()