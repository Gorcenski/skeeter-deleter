import argparse
import dateutil.parser
import httpx
import magic
import os
import rich.progress
from atproto import CAR, Client, models
from atproto_client.request import Request
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from functools import partial
from pathlib import Path
import logging
import unittest
import keyring

# Configuration du logging
logging.basicConfig(filename='skeeter_deleter.log', level=logging.INFO,
                    format='%(asctime)s %(levelname)s:%(message)s')

class PostQualifier(models.AppBskyFeedDefs.FeedViewPost):
    def is_viral(self, viral_threshold) -> bool:
        """
        Check if the post is viral based on the repost count.

        Args:
            viral_threshold (int): The threshold for considering a post viral.

        Returns:
            bool: True if the post is viral, False otherwise.
        """
        if viral_threshold == 0:
            return False
        return self.post.repost_count >= viral_threshold

    def is_stale(self, stale_threshold, now) -> bool:
        """
        Check if the post is stale based on its age.

        Args:
            stale_threshold (int): The threshold for considering a post stale.
            now (datetime): The current time.

        Returns:
            bool: True if the post is stale, False otherwise.
        """
        if stale_threshold == 0:
            return False
        return dateutil.parser.parse(self.post.record.created_at).replace(tzinfo=timezone.utc) <= \
            now - timedelta(days=stale_threshold)

    def is_protected_domain(self, domains_to_protect) -> bool:
        """
        Check if the post contains links to protected domains.

        Args:
            domains_to_protect (list): List of domains to protect.

        Returns:
            bool: True if the post contains links to protected domains, False otherwise.
        """
        return hasattr(self.post.embed, "external") and \
            any([uri in self.post.embed.external.uri for uri in domains_to_protect])

    def is_self_liked(self) -> bool:
        """
        Check if the author of the post has liked it.

        Returns:
            bool: True if the author has liked the post, False otherwise.
        """
        lc = None
        while True:
            try:
                likes = self.client.app.bsky.feed.get_likes(params={
                    'uri': self.post.uri,
                    'cursor': lc,
                    'limit': 100})
                lc = likes.cursor
                if self.client.me.did in [l.actor.did for l in likes.likes] and \
                    self.post.author.did == self.client.me.did:
                    return True
                if not lc:
                    break
            except httpx.HTTPStatusError as e:
                logging.error(f"HTTP error occurred: {e}")
            except Exception as e:
                logging.error(f"An error occurred: {e}")
        return False

    def __init__(self, client: Client):
        super(PostQualifier, self).__init__()
        self._init_PostQualifier(client)

    def _init_PostQualifier(self, client: Client):
        self.client = client

    def delete_like(self):
        """
        Delete a like from the post.
        """
        try:
            self.client.delete_like(self.post.viewer.like)
        except httpx.HTTPStatusError as e:
            logging.error(f"HTTP error occurred: {e}")
        except Exception as e:
            logging.error(f"An error occurred: {e}")

    def remove(self):
        """
        Remove the post or unrepost it.
        """
        if self.post.author.did != self.client.me.did:
            try:
                self.client.unrepost(self.post.viewer.repost)
            except httpx.HTTPStatusError as e:
                logging.error(f"HTTP error occurred: {e}")
            except Exception as e:
                logging.error(f"An error occurred: {e}")
        else:
            if self.confirm_delete(self.post):
                try:
                    self.client.delete_post(self.post.uri)
                except httpx.HTTPStatusError as e:
                    logging.error(f"HTTP error occurred: {e}")
                except Exception as e:
                    logging.error(f"An error occurred: {e}")

    def confirm_delete(self, post):
        """
        Confirm the deletion of a post.

        Args:
            post (PostQualifier): The post to delete.

        Returns:
            bool: True if the deletion is confirmed, False otherwise.
        """
        confirmation = input(f"Are you sure you want to delete the post: {post.post.uri}? (yes/no): ")
        return confirmation.lower() == 'yes'

    @staticmethod
    def to_delete(viral_threshold, stale_threshold, domains_to_protect, now, post):
        """
        Determine if a post should be deleted.

        Args:
            viral_threshold (int): The threshold for considering a post viral.
            stale_threshold (int): The threshold for considering a post stale.
            domains_to_protect (list): List of domains to protect.
            now (datetime): The current time.
            post (PostQualifier): The post to evaluate.

        Returns:
            bool: True if the post should be deleted, False otherwise.
        """
        if (post.is_viral(viral_threshold) or post.is_stale(stale_threshold, now)) and \
            not post.is_protected_domain(domains_to_protect) and \
            not post.is_self_liked():
            return True
        return False

    @staticmethod
    def to_unlike(stale_threshold, now, post):
        """
        Determine if a post should be unliked.

        Args:
            stale_threshold (int): The threshold for considering a post stale.
            now (datetime): The current time.
            post (PostQualifier): The post to evaluate.

        Returns:
            bool: True if the post should be unliked, False otherwise.
        """
        return post.is_stale(stale_threshold, now) and \
            not post.is_self_liked()

    @staticmethod
    def cast(client: Client, post: models.AppBskyFeedDefs.FeedViewPost):
        """
        Cast a post to a PostQualifier instance.

        Args:
            client (Client): The ATProto client.
            post (models.AppBskyFeedDefs.FeedViewPost): The post to cast.

        Returns:
            PostQualifier: The casted post.
        """
        post.__class__ = PostQualifier
        post._init_PostQualifier(client)
        return post

@dataclass
class Credentials:
    login: str
    password: str

    dict = asdict

class RequestCustomTimeout(Request):
    def __init__(self, timeout: httpx.Timeout = httpx.Timeout(120), *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._client = httpx.Client(follow_redirects=True, timeout=timeout)

class SkeeterDeleter:

    def gather_posts_to_unlike(self, stale_threshold, now, fixed_likes_cursor, **kwargs) -> list[PostQualifier]:
        """
        Gather posts to unlike.

        Args:
            stale_threshold (int): The threshold for considering a post stale.
            now (datetime): The current time.
            fixed_likes_cursor (str): The cursor for fixed likes.

        Returns:
            list[PostQualifier]: List of posts to unlike.
        """
        cursor = None
        to_unlike = []
        while True:
            try:
                posts = self.client.app.bsky.feed.get_actor_likes(params={
                    "actor": self.client.me.handle,
                    "cursor": cursor,
                    "limit": 100
                    })
                to_unlike.extend(list(filter(partial(PostQualifier.to_unlike, stale_threshold, now),
                                             map(partial(PostQualifier.cast, self.client), posts.feed))))

                if cursor == posts.cursor or (fixed_likes_cursor and posts.cursor < fixed_likes_cursor):
                    break
                else:
                    cursor = posts.cursor
                    if self.verbosity > 0:
                        print(cursor)
            except httpx.HTTPStatusError as e:
                logging.error(f"HTTP error occurred: {e}")
            except Exception as e:
                logging.error(f"An error occurred: {e}")
        return to_unlike

    def gather_posts_to_delete(self, viral_threshold, stale_threshold, domains_to_protect, now, **kwargs) -> list[PostQualifier]:
        """
        Gather posts to delete.

        Args:
            viral_threshold (int): The threshold for considering a post viral.
            stale_threshold (int): The threshold for considering a post stale.
            domains_to_protect (list): List of domains to protect.
            now (datetime): The current time.

        Returns:
            list[PostQualifier]: List of posts to delete.
        """
        cursor = None
        to_delete = []
        while True:
            try:
                posts = self.client.get_author_feed(self.client.me.handle,
                                                    cursor=cursor,
                                                    filter="from:me",
                                                    limit=100)
                delete_test = partial(PostQualifier.to_delete, viral_threshold, stale_threshold, domains_to_protect, now)
                to_delete.extend(list(filter(delete_test,
                                            map(partial(PostQualifier.cast, self.client), posts.feed))))

                cursor = posts.cursor
                if self.verbosity > 0:
                    print(cursor)
                if not cursor:
                    break
            except httpx.HTTPStatusError as e:
                logging.error(f"HTTP error occurred: {e}")
            except Exception as e:
                logging.error(f"An error occurred: {e}")
        return to_delete

    def batch_unlike_posts(self) -> None:
        """
        Unlike posts in batch.
        """
        if self.verbosity > 0:
            print(f"Unliking {len(self.to_unlike)} post{'' if len(self.to_unlike) == 1 else 's'}")
        for post in rich.progress.track(self.to_unlike):
            if self.verbosity == 2:
                print(f"Unliking: {post.post.record.post} by {post.post.author.handle}, CID: {post.post.cid}")
            post.delete_like()

    def batch_delete_posts(self) -> None:
        """
        Delete posts in batch.
        """
        if self.verbosity > 0:
            print(f"Deleting {len(self.to_delete)} post{'' if len(self.to_delete) == 1 else 's'}")
        for post in rich.progress.track(self.to_delete):
            if self.verbosity == 2:
                print(f"Deleting: {post.post.record.post} on {post.post.record.created_at}, CID: {post.post.cid}")
            post.remove()

    def archive_repo(self, now, **kwargs):
        """
        Archive the repository.

        Args:
            now (datetime): The current time.
        """
        try:
            repo = self.client.com.atproto.sync.get_repo(params={'did': self.client.me.did})
            car = CAR.from_bytes(repo)
            clean_user_did = self.client.me.did.replace(":", "_")
            Path(f"archive/{clean_user_did}/_blob/").mkdir(parents=True, exist_ok=True)
            print("Archiving posts...")
            clean_now = now.isoformat().replace(':','_')
            with open(f"archive/{clean_user_did}/bsky-archive-{clean_now}.car", "wb") as f:
                f.write(repo)

            cursor = None
            print("Downloading and archiving media...")
            blob_cids = []
            while True:
                blob_page = self.client.com.atproto.sync.list_blobs(params={'did': self.client.me.did, 'cursor': cursor})
                blob_cids.extend(blob_page.cids)
                cursor = blob_page.cursor
                if not cursor:
                    break
            for cid in rich.progress.track(blob_cids):
                blob = self.client.com.atproto.sync.get_blob(params={'cid': cid, 'did': self.client.me.did})
                type = magic.from_buffer(blob, 2048)
                ext = ".jpeg" if type == "image/jpeg" else ""
                with open(f"archive/{clean_user_did}/_blob/{cid}{ext}", "wb") as f:
                    if self.verbosity == 2:
                        print(f"Saving {cid}{ext}")
                    f.write(blob)
        except httpx.HTTPStatusError as e:
            logging.error(f"HTTP error occurred: {e}")
        except Exception as e:
            logging.error(f"An error occurred: {e}")

    def __init__(self,
                 credentials: Credentials,
                 viral_threshold: int = 0,
                 stale_threshold: int = 0,
                 domains_to_protect: list[str] = [],
                 fixed_likes_cursor: str = None,
                 verbosity: int = 0,
                 autodelete: bool = False):
        self.client = Client(request=RequestCustomTimeout())
        self.client.login(**credentials.dict())

        params = {
            'viral_threshold': viral_threshold,
            'stale_threshold': stale_threshold,
            'domains_to_protect': domains_to_protect,
            'fixed_likes_cursor': fixed_likes_cursor,
            'now': datetime.now(timezone.utc),
        }
        self.verbosity = verbosity
        self.autodelete = autodelete

        self.archive_repo(**params)

        self.to_unlike = self.gather_posts_to_unlike(**params)
        print(f"Found {len(self.to_unlike)} post{'' if len(self.to_unlike) == 1 else 's'} to unlike.")

        self.to_delete = self.gather_posts_to_delete(**params)
        print(f"Found {len(self.to_delete)} post{'' if len(self.to_delete) == 1 else 's'} to delete.")

    def unlike(self):
        """
        Unlike posts after confirmation.
        """
        n_unlike = len(self.to_unlike)
        prompt = None
        while not self.autodelete and prompt not in ("Y", "n"):
            prompt = input(f"""
Proceed to unlike {n_unlike} post{'' if n_unlike == 1 else 's'}? WARNING: THIS IS DESTRUCTIVE AND CANNOT BE UNDONE. Y/n: """)
        if self.autodelete or prompt == "Y":
            self.batch_unlike_posts()

    def delete(self):
        """
        Delete posts after confirmation.
        """
        n_delete = len(self.to_delete)
        prompt = None
        while not self.autodelete and prompt not in ("Y", "n"):
            prompt = input(f"""
Proceed to delete {n_delete} post{'' if n_delete == 1 else 's'}? WARNING: THIS IS DESTRUCTIVE AND CANNOT BE UNDONE. Y/n: """)
        if self.autodelete or prompt == "Y":
            self.batch_delete_posts()

def get_credentials():
    """
    Get credentials from keyring.

    Returns:
        Credentials: The credentials.
    """
    login = keyring.get_password("bluesky", "login")
    password = keyring.get_password("bluesky", "password")
    return Credentials(login, password)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("-l", "--max-reposts", help="The upper bound of the number of reposts a post can have before it is deleted.", default=0, type=int)
    parser.add_argument("-s", "--stale-limit", help="The upper bound of the age of a post in days before it is deleted.", default=0, type=int)
    parser.add_argument("-d", "--domains-to-protect", help="A comma separated list of domain names to protect.", default="")
    parser.add_argument("-c", "--fixed-likes-cursor", help="A complex setting for pagination.", default="")
    verbosity = parser.add_mutually_exclusive_group()
    verbosity.add_argument("-v", "--verbose", help="Show more information about what is happening.", action="store_true")
    verbosity.add_argument("-vv", "--very-verbose", help="Show granular information about what is happening.", action="store_true")
    parser.add_argument("-y", "--yes", help="Ignore warning prompts for deletion.", action="store_true", default=False)
    args = parser.parse_args()

    creds = get_credentials()
    verbosity = 0
    if args.verbose:
        verbosity = 1
    elif args.very_verbose:
        verbosity = 2
    params = {
        'viral_threshold': max([0, args.max_reposts]),
        'stale_threshold': max([0, args.stale_limit]),
        'domains_to_protect': [s.strip() for s in args.domains_to_protect.split(",")],
        'fixed_likes_cursor': args.fixed_likes_cursor,
        'verbosity': verbosity,
        'autodelete': args.yes
    }
    sd = SkeeterDeleter(credentials=creds, **params)
    sd.unlike()
    sd.delete()

class TestPostQualifier(unittest.TestCase):
    def test_is_viral(self):
        post = PostQualifier(client=None)
        post.post.repost_count = 10
        self.assertTrue(post.is_viral(5))
        self.assertFalse(post.is_viral(15))

    def test_is_stale(self):
        post = PostQualifier(client=None)
        post.post.record.created_at = "2023-01-01T00:00:00Z"
        now = datetime(2023, 1, 11, tzinfo=timezone.utc)
        self.assertTrue(post.is_stale(10, now))
        self.assertFalse(post.is_stale(5, now))

if __name__ == '__main__':
    unittest.main()
