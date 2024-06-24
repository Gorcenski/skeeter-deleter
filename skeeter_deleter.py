import argparse
import dateutil.parser
import magic
import os
import pytz
from atproto import CAR, Client, models
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from functools import partial
from pathlib import Path

class PostQualifier(models.AppBskyFeedDefs.FeedViewPost):
    # This class wraps the ATProto FeedViewPost instance of a post, as returned from the feed
    # The rationale here is to separate post-related business logic (e.g. age heuristics, virality)
    # from feed-related business logic
    #
    # These filters are customizable, or new filters can be added here
    def is_viral(self, viral_threshold) -> bool:
        if viral_threshold == 0:
            return False
        return self.post.repost_count >= viral_threshold

    def is_stale(self, stale_threshold, now) -> bool:
        if stale_threshold == 0:
            return False
        return dateutil.parser.parse(self.post.record.created_at) <= \
            now - timedelta(days=stale_threshold)

    def is_protected_domain(self, domains_to_protect) -> bool:
        return hasattr(self.post.embed, "external") and \
            any([uri in self.post.embed.external.uri for uri in domains_to_protect])
        
    def is_self_liked(self) -> bool:
        # This looks through a post's likes to see if the author herself liked it, so that it won't
        # be deleted. This is used for both likes and posts, and it's probably inefficient to do so.
        # A candidate for a future refactor.
        lc = None
        while True:
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
        return False
    
    def __init__(self, client : Client):
        super(PostQualifier, self).__init__()
        self._init_PostQualifier(client)
    
    def _init_PostQualifier(self, client : Client):
        self.client = client

    def delete_like(self):
        self.client.delete_like(self.post.viewer.like)

    def remove(self):
        if self.post.author.handle != self.client.me.did:
            self.client.unrepost(self.post.viewer.repost)
        else:
            self.client.delete_post(self.post.uri)

    @staticmethod
    def to_delete(viral_threshold, stale_threshold, domains_to_protect, now, post):
        if (post.is_viral(viral_threshold) or post.is_stale(stale_threshold, now)) and \
            not post.is_protected_domain(domains_to_protect) and \
            not post.is_self_liked():
            return True
        return False

    @staticmethod
    def to_unlike(stale_threshold, now, post):
        return post.is_stale(stale_threshold, now) and \
            not post.is_self_liked()
    
    @staticmethod
    def cast(client : Client, post : models.AppBskyFeedDefs.FeedViewPost):
        post.__class__ = PostQualifier
        post._init_PostQualifier(client)
        return post

@dataclass
class Credentials:
    login: str
    password: str

    dict = asdict

class SkeeterDeleter:

    def gather_posts_to_unlike(self, stale_threshold, now, fixed_likes_cursor, **kwargs) -> list[PostQualifier]:
        cursor = None
        to_unlike = []
        while True:
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
                if verbosity > 0:
                    print(cursor)
        return to_unlike

    def gather_posts_to_delete(self, viral_threshold, stale_threshold, domains_to_protect, now, **kwargs) -> list[PostQualifier]:
        cursor = None
        to_delete = []
        while True:
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
            if cursor == None:
                break
        return to_delete

    def batch_unlike_posts(self) -> None:
        if self.verbosity > 0:
            print(f"Unliking {len(self.to_unlike)} post{'' if len(self.to_unlike) == 1 else 's'}")
        for post in self.to_unlike:
            if self.verbosity == 2:
                print(f"Unliking: {post.post.record.post} by {post.post.author.handle}, CID: {post.post.cid}")
            post.delete_like()

    def batch_delete_posts(self) -> None:
        if self.verbosity > 0:
            print(f"Deleting {len(self.to_unlike)} post{'' if len(self.to_unlike) == 1 else 's'}")
        for post in self.to_delete:
            if self.verbosity == 2:
                print(f"Deleting: {post.post.record.post} on {post.post.record.created_at}, CID: {post.post.cid}")
            post.remove()
            
    def archive_repo(self, now, **kwargs):
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
        while True:
            blobs = self.client.com.atproto.sync.list_blobs(params={'did': self.client.me.did, 'cursor': cursor})
            for cid in blobs.cids:
                blob = self.client.com.atproto.sync.get_blob(params={'cid': cid, 'did': self.client.me.did})
                type = magic.from_buffer(blob, 2048)
                ext = ".jpeg" if type == "image/jpeg" else ""
                with open(f"archive/{clean_user_did}/_blob/{cid}{ext}", "wb") as f:
                    if self.verbosity == 2:
                        print(f"Saving {cid}{ext}")
                    f.write(blob)
            cursor = blobs.cursor
            if not cursor:
                break

    def __init__(self,
                 credentials : Credentials,
                 viral_threshold : int=0,
                 stale_threshold : int=0,
                 domains_to_protect : list[str]=[],
                 fixed_likes_cursor : str=None,
                 verbosity : int=0,
                 autodelete : bool=False):
        self.client = Client()
        self.client.login(**credentials.dict())

        # the parameters are a mess, sorry, this is a to-fix
        params = {
            'viral_threshold': viral_threshold,
            'stale_threshold': stale_threshold,
            'domains_to_protect': domains_to_protect,
            'fixed_likes_cursor': fixed_likes_cursor,
            'now': datetime.now(pytz.UTC),
        }
        self.verbosity = verbosity
        self.autodelete = autodelete

        self.archive_repo(**params)

        self.to_unlike = self.gather_posts_to_unlike(**params)
        print(f"Found {len(self.to_unlike)} post{'' if len(self.to_unlike) == 1 else 's'} to unlike.")

        self.to_delete = self.gather_posts_to_delete(**params)
        print(f"Found {len(self.to_delete)} post{'' if len(self.to_delete) == 1 else 's'} to delete.")

    def unlike(self):
        n_unlike = len(self.to_unlike)
        if not self.autodelete:
            prompt = input(f"""
Proceed to unlike {n_unlike} post{'' if n_unlike == 1 else 's'}? WARNING: THIS IS DESTRUCTIVE AND CANNOT BE UNDONE. Y/n: """)
        if self.autodelete or prompt == "Y":
            sd.batch_unlike_posts()

    def delete(self):
        n_delete = len(self.to_delete)
        if not self.autodelete:
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
misinterpreted, reducing potential harassment. Detaults to 0.""", default=0, type=int)
    parser.add_argument("-d", "--domains-to-protect", help="""A comma separated list of domain names to protect. Posts linking to
domains in this list will not be auto-deleted regardless of age or virality. Default is empty.""", default=[])
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
        'domains_to_protect': [s.strip() for s in args.domains_to_protect.split(",")],
        'fixed_likes_cursor': args.fixed_likes_cursor,
        'verbosity': verbosity,
        'autodelete': args.yes
    }
    sd = SkeeterDeleter(credentials=creds, **params)
    sd.unlike()
    sd.delete()
