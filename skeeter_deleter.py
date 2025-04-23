import argparse
import dateutil.parser
import httpx
import logging
import magic
import os
import rich.progress
from atproto import CAR, Client, models
from atproto_core.cid import CID
from atproto_client.request import Request
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from functools import partial
from pathlib import Path

logging.basicConfig(filename='skeeter_deleter.log', level=logging.INFO,
                    format='%(asctime)s %(levelname)s:%(message)s')

class PostQualifier(models.AppBskyFeedDefs.PostView):
    """
    This class wraps the ATProto PostView instance of a post, as returned from a list of posts
    The rationale here is to separate post-related business logic (e.g. age heuristics, virality)
    from feed-related business logic
    
    These filters are customizable, or new filters can be added here
    """
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
        return self.repost_count >= viral_threshold

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
        return dateutil.parser.parse(self.record.created_at).replace(tzinfo=timezone.utc) <= \
            now - timedelta(days=stale_threshold)

    def is_protected_domain(self, domains_to_protect) -> bool:
        """
        Check if the post contains links to protected domains.
        Args:
            domains_to_protect (list): List of domains to protect.
        Returns:
            bool: True if the post contains links to protected domains, False otherwise.
        """
        return hasattr(self.embed, "external") and \
            any([uri in self.embed.external.uri for uri in domains_to_protect])
        
    def is_self_liked(self, self_likes) -> bool:
        """
        Check if the author of the post has liked it.

        Args:
            self_likes (list): a list of self-liked posts, extracted from
                               the feed archive
        Returns:
            bool: True if the author has liked the post, False otherwise.
        """
        return self.uri in [post['subject']['uri'] for post in self_likes]
    
    def __init__(self, client : Client):
        super(PostQualifier, self).__init__()
        self._init_PostQualifier(client)
    
    def _init_PostQualifier(self, client : Client):
        self.client = client

    def delete_like(self):
        """
        Remove a like from a post
        """
        try:
            logging.info(f"Removing like: {self.viewer.like}")
            self.client.delete_like(self.viewer.like)
        except httpx.HTTPStatusError as e:
            logging.error(f"HTTP error occurred while unliking: {e}")
            try:
                logging.info(f"Removing like via URI: {self.uri}")
                self.client.delete_like(self.uri)
            except httpx.HTTPStatusError as e:
                logging.error(f"HTTP error occurred while unliking via URI: {e}")
            except Exception as e:
                raise e
        except Exception as e:
            logging.error(f"An error occurred while unliking: {e}")

    def remove(self):
        """
        Remove a repost or delete an authored post
        """
        if self.author.did != self.client.me.did:
            try:
                logging.info(f"Removing repost: {self.viewer.repost}")
                self.client.unrepost(self.viewer.repost)
            except httpx.HTTPStatusError as e:
                logging.error(f"HTTP error occurred during unreposting: {e}")
            except Exception as e:
                logging.error(f"An error occurred during unreposting: {e}")
        else:
            try:
                logging.info(f"Removing post: {self.uri}")
                self.client.delete_post(self.uri)
            except httpx.HTTPStatusError as e:
                logging.error(f"HTTP error occurred during deletion: {e}")
            except Exception as e:
                logging.error(f"An error occurred during deletion: {e}")

    @staticmethod
    def to_delete(viral_threshold, stale_threshold, domains_to_protect, now, self_likes, post):
        """
        Determine if a post should be deleted.
        Args:
            viral_threshold (int): The threshold for considering a post viral.
            stale_threshold (int): The threshold for considering a post stale.
            domains_to_protect (list): List of domains to protect.
            now (datetime): The current time.
            self_likes (list): List of self-liked posts extracted from
                               the feed archive
            post (PostQualifier): The post to evaluate.
        Returns:
            bool: True if the post should be deleted, False otherwise.
        """
        if (post.is_viral(viral_threshold) or post.is_stale(stale_threshold, now)) and \
            not post.is_protected_domain(domains_to_protect) and \
            not post.is_self_liked(self_likes):
            return True
        return False

    @staticmethod
    def to_remove(stale_threshold, now, post):
        """
        Determine if a post should be unliked.
        Args:
            stale_threshold (int): The threshold for considering a post stale.
            now (datetime): The current time.
            post (PostQualifier): The post to evaluate.
        Returns:
            bool: True if the post should be unliked, False otherwise.
        """
        return post.is_stale(stale_threshold, now)
    
    @staticmethod
    def cast(client : Client, post : models.AppBskyFeedDefs.PostView):
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
    @staticmethod
    def chunker(seq, size : int):
        """
        Break a iterable into segments of a given size

        Args:
            seq (iterable): The iterable to be broken into chunks
            size (int): The chunk size
        Returns:
            list[iterable]: List of iterables of length at most size
        """
        return (seq[pos:pos + size] for pos in range(0, len(seq), size))
    
    @staticmethod
    def extract_feed_item(archive, block):
        """
        Converts feed items from the repo with various structures into consistent blocks

        Args:
            archive: The repository as a binary CAR
            block: The block to extract
        Returns:
            block: A decoded block
        """
        if '$type' in block:
            return block
        elif 'e' in block and len(block['e']) > 0:
            return archive.blocks.get(CID.decode(block['e'][0]['v']))
        else:
            return block

    def gather_likes(self,
                     repo,
                     stale_threshold,
                     now,
                     **kwargs) -> list[PostQualifier]:
        archive = CAR.from_bytes(repo)
        likes = list(map(partial(self.extract_feed_item, archive),
                         filter(lambda x: 'app.bsky.feed.like' in str(x),
                                [archive.blocks.get(cid) for cid in archive.blocks])))
        
        self_likes = list(filter(lambda x: archive.blocks.get(x['subject']['cid']),
                                 filter(lambda x : self.client.me.did in x['subject']['uri'], likes)))
        other_likes = list(filter(lambda x : self.client.me.did not in x['subject']['uri'], likes))
        
        # The API limits the get_posts method to 25 results at a time. 
        to_unlike = []
        for batch in self.chunker(other_likes, 25):
            try:
                posts_to_unlike = self.client.get_posts(uris=[x['subject']['uri']
                                                            for x in batch])
                to_unlike.extend(
                    list(filter(
                        partial(PostQualifier.to_remove, stale_threshold, now),
                        map(partial(PostQualifier.cast, self.client),
                            posts_to_unlike.posts)
                    ))
                )
            except httpx.HTTPStatusError as e:
                logging.error(f"An HTTP error occured while fetching likes: {e}")
            except Exception as e:
                logging.error(f"An error occured while fetching likes: {e}")

        return self_likes, to_unlike

    def gather_reposts(self,
                       repo,
                       viral_threshold,
                       stale_threshold,
                       domains_to_protect,
                       now,
                       self_likes,
                       **kwargs) -> list[PostQualifier]:
        archive = CAR.from_bytes(repo)
        
        reposts = list(
            filter(lambda x : '$type' in x and
                   "app.bsky.feed.repost" in str(x),
                   [archive.blocks.get(cid) for cid in archive.blocks]
            )
        )
        to_unrepost = []
        for batch in self.chunker(reposts, 25):
            try:
                posts_to_remove = self.client.get_posts(uris=[x['subject']['uri']
                                                            for x in batch])
                to_unrepost.extend(
                    list(filter(
                        partial(PostQualifier.to_delete,
                                viral_threshold,
                                stale_threshold,
                                domains_to_protect,
                                now,
                                self_likes),
                        map(partial(PostQualifier.cast, self.client),
                            posts_to_remove.posts)
                    ))
                )
            except httpx.HTTPStatusError as e:
                logging.error(f"An HTTP error occured while fetching reposts: {e}")
            except Exception as e:
                logging.error(f"An error occured while fetching reposts: {e}")
        return to_unrepost

    def gather_posts_to_delete(self,
                               viral_threshold,
                               stale_threshold,
                               domains_to_protect,
                               now,
                               self_likes,
                               **kwargs) -> list[PostQualifier]:
        cursor = None
        to_delete = []
        while True:
            try:
                posts = self.client.get_author_feed(self.client.me.handle,
                                                    cursor=cursor,
                                                    filter="from:me",
                                                    limit=100)
                delete_test = partial(PostQualifier.to_delete,
                                    viral_threshold,
                                    stale_threshold,
                                    domains_to_protect,
                                    now,
                                    self_likes)
                to_delete.extend(list(filter(
                    delete_test,
                    map(partial(PostQualifier.cast, self.client),
                        [x.post for x in posts.feed]
                    )
                )))

                cursor = posts.cursor
                if self.verbosity > 0:
                    print(f"Cursor at: {cursor}")
            except httpx.HTTPStatusError as e:
                logging.error(f"An HTTP error occured while fetching posts: {e}")
            except Exception as e:
                logging.error(f"An error occured while fetching posts: {e}")
            if cursor == None:
                break
        return to_delete

    def batch_unlike_posts(self) -> None:
        logging.info(f"Unliking {len(self.to_unlike)} post{'' if len(self.to_unlike) == 1 else 's'}")
        if self.verbosity > 0:
            print(f"Unliking {len(self.to_unlike)} post{'' if len(self.to_unlike) == 1 else 's'}")
        for post in rich.progress.track(self.to_unlike):
            logging.info(f"Unliking: {post.uri} by {post.author.handle}, CID: {post.cid}")
            if self.verbosity == 2:
                print(f"Unliking: {post.uri} by {post.author.handle}, CID: {post.cid}")
            post.delete_like()

    def batch_delete_posts(self) -> None:
        logging.info(f"Deleting {len(self.to_delete)} post{'' if len(self.to_delete) == 1 else 's'}")
        if self.verbosity > 0:
            print(f"Deleting {len(self.to_delete)} post{'' if len(self.to_delete) == 1 else 's'}")
        for post in rich.progress.track(self.to_delete):
            logging.info(f"Deleting: {post.record.text} on {post.record.created_at}, CID: {post.cid}")
            if self.verbosity == 2:
                print(f"Deleting: {post.record.text} on {post.record.created_at}, CID: {post.cid}")
            post.remove()
            
    def archive_repo(self, now, **kwargs):
        repo = self.client.com.atproto.sync.get_repo(params={'did': self.client.me.did})
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

        return repo

    def __init__(self,
                 credentials : Credentials,
                 viral_threshold : int=0,
                 stale_threshold : int=0,
                 domains_to_protect : list[str]=[],
                 fixed_likes_cursor : str=None,
                 verbosity : int=0,
                 autodelete : bool=False):
        self.client = Client(request=RequestCustomTimeout())
        self.client.login(**credentials.dict())

        # the parameters are a mess, sorry, this is a to-fix
        params = {
            'viral_threshold': viral_threshold,
            'stale_threshold': stale_threshold,
            'domains_to_protect': domains_to_protect,
            'fixed_likes_cursor': fixed_likes_cursor,
            'now': datetime.now(timezone.utc),
        }
        self.verbosity = verbosity
        self.autodelete = autodelete

        repo = self.archive_repo(**params)

        self_likes, self.to_unlike = self.gather_likes(repo, **params)
        print(f"Found {len(self.to_unlike)} post{'' if len(self.to_unlike) == 1 else 's'} to unlike.")
        
        to_unrepost = self.gather_reposts(repo, self_likes=self_likes, **params)
        print(f"Found {len(to_unrepost)} post{'' if len(to_unrepost) == 1 else 's'} to unrepost.")

        self.to_delete = self.gather_posts_to_delete(self_likes=self_likes, **params)
        print(f"Found {len(self.to_delete)} post{'' if len(self.to_delete) == 1 else 's'} to delete.")

        self.to_delete.extend(to_unrepost)


    def unlike(self):
        n_unlike = len(self.to_unlike)
        prompt = None
        while not self.autodelete and prompt not in ("Y", "n"):
            prompt = input(f"""
Proceed to unlike {n_unlike} post{'' if n_unlike == 1 else 's'}? WARNING: THIS IS DESTRUCTIVE AND CANNOT BE UNDONE. Y/n: """)
        if self.autodelete or prompt == "Y":
            sd.batch_unlike_posts()

    def delete(self):
        n_delete = len(self.to_delete)
        prompt = None
        while not self.autodelete and prompt not in ("Y", "n"):
            prompt = input(f"""
Proceed to delete {n_delete} post{'' if n_delete == 1 else 's'}? WARNING: THIS IS DESTRUCTIVE AND CANNOT BE UNDONE. Y/n: """)
        if self.autodelete or prompt == "Y":
            sd.batch_delete_posts()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("-l", "--max-reposts", help="""The upper bound of the number of reposts a post can have before it is deleted.
Ignore or set to 0 to not set an upper limit. This feature deletes posts that are going viral, which can reduce harassment.
Defaults to 0.""", default=0, type=int)
    parser.add_argument("-s", "--stale-limit", help="""The upper bound of the age of a post in days before it is deleted.
Ignore or set to 0 to not set an upper limit. This feature deletes old posts that may be taken out of context or selectively
misinterpreted, reducing potential harassment. Defaults to 0.""", default=0, type=int)
    parser.add_argument("-d", "--domains-to-protect", help="""A comma separated list of domain names to protect. Posts linking to
domains in this list will not be auto-deleted regardless of age or virality. Default is empty.""", default="")
    parser.add_argument("-c", "--fixed-likes-cursor", help="""A complex setting. ATProto pagination through is awkward, and
it will page through the entire history of your account even if there are no likes to be found. This can make the process take
a long time to complete. If you have already purged likes, it's possible to simply set a token at a reasonable point in the recent
past which will terminate the search. To list the tokens, run -vv mode. Tokens are short alphanumeric strings. Default empty.""",
default="")
    verbosity = parser.add_mutually_exclusive_group()
    verbosity.add_argument("-v", "--verbose", help="""Show more information about what is happening.""",
                           action="store_true")
    verbosity.add_argument("-vv", "--very-verbose", help="""Show granular information about what is happening.""",
                           action="store_true")
    parser.add_argument("-y", "--yes", help="""Ignore warning prompts for deletion. Necessary for running in automation.""",
                        action="store_true", default=False)
    args = parser.parse_args()

    creds = Credentials(os.environ["BLUESKY_USERNAME"],
                        os.environ["BLUESKY_PASSWORD"])
    verbosity = 0
    if args.verbose:
        verbosity = 1
    elif args.very_verbose:
        verbosity = 2
    params = {
        'viral_threshold': max([0, args.max_reposts]),
        'stale_threshold': max([0, args.stale_limit]),
        'domains_to_protect': ([] if args.domains_to_protect == ""
                               else [s.strip() 
                                     for s in args.domains_to_protect.split(",")]),
        'fixed_likes_cursor': args.fixed_likes_cursor,
        'verbosity': verbosity,
        'autodelete': args.yes
    }

    sd = SkeeterDeleter(credentials=creds, **params)
    sd.unlike()
    sd.delete()
