# skeeter-deleter
A github action for auto-deleting Bluesky posts.

THIS CODE IS VERY DESTRUCTIVE. The maintainer assumes no liability or warranty for its use. Use at your own peril.

## What it is

This is a command-line tool that allows a user to download an archive of their Bluesky posts, remove likes, and delete old posts per user-specified criteria, namely age and virality.

### Why delete old posts?

Social media is a great tool to connect people, but it can also be a tool to harass and harm people. One of the more insidious social media behaviors is the tendency to dig up old, out-of-context posts and use them to impugn, impeach, harass, molest, or otherwise defame other users. Similarly, posts that go viral tend to attract unwanted attention, which can lead to arguments, harassment, or other unpleasant experiences.

This tool helps purge your profile so this can't happen, while also preserving a private archive of the posts for any future needs.

### What if I want to keep some posts?

The tool allows you to create a curated feed of your content. Simply "like" your own post and it will be marked for preservation. Beware: if you unlike the post, it may be deleted if you run this script again.

### How does it work?

The tool works as follows:

1. it downloads a CAR archive and all embedded media and stores it to an `archive` folder locally
1. it reads your account's likes feed and gathers likes of a certain configurable age
1. it reads your account's posts, replies, and reposts and gathers all such items of a certain configurable age or popularity, as measured by the number of reblogs
1. it unlikes posts
1. it deletes posts

## Installation

Clone this repository and install the python libraries from `requirements.txt` using your preferred python package management solution. Note: you will need to install `libmagic`. Please see the instructions on [the `python-magic` pypi page](https://pypi.org/project/python-magic/).

You'll need to set environment variables, `BLUESKY_USERNAME` and `BLUESKY_PASSWORD` in your OS. Unfortunately, Bluesky does not currently support a more secure AuthN solution such as OAuth at this time.

## Running

Activate your environment.

Run the command with

```sh
python skeeter-deleter.py
```

with the following command line options:

- `-l`, `--max-reposts`: the upper limit of reposts a post can have before it is deleted. This is to prevent post virality by deleting posts once they've grown too popular. Set this to 0 or ignore the flag if you don't want to use this feature. Note: at least either `-s` or `-l` (or both) must be set to delete posts, replies, reblogs, or likes.

- `-s`, `--stale-limit`: the upper limit of the age in days a post can be. This is to prevent people digging up old posts. Set this to 0 or ignore the flag if you don't want to use this feature. Note: at least either `-s` or `-l` (or both) must be set to delete posts, replies, reblogs, or likes.

- `-d`, `--domains-to-protect`: a comma-separated list of domain names to preserve, for example you can configure the tool to not delete links to your blog or your favorite sites. Optional.

- `-c`, `--fixed-likes-cursor`: the cursor ID for the maximum lookback for likes. Due to the ATProto design, fetching likes can take a long time, as the cursor still pages even if there are no old likes to be found. If you have run this tool at least once before, it is recommended to set this to a cursor in the recent past. This can be obtained by running `-v` or `-vv` and copying the cursor output

- `-v`, `--verbose`: emit more information about progress, useful for initial runs where many posts will be archived and deleted

- `-vv`, `--very-verbose`: emit granular information about each post. Not recommended.

- `-y`, `--yes`: automatically answer yes to all warnings about deleting posts, necessary for use in automation.

### Automating

This could be run in a docker container in a Github action, in a cloud function, or in any other scheduled environment.

### Dockerized scheduler

The repository now ships with a small scheduler stack so you can run Skeeter Deleter in a long-lived container without wiring up your own cron job.

1. Copy `.env.sample` to `.env` and fill in the required variables (`BLUESKY_USERNAME`, `BLUESKY_PASSWORD`, and at least one of `STALE_LIMIT_DAYS`/`MAX_REPOSTS`). Optional tuning flags in this file map directly to the CLI flags listed above.
2. Build and launch the container with Docker Compose:
   ```sh
   docker compose up --build
   ```
   Add `-d` if you want it to run in the background.

## Future Roadmap

A few features are planned:

- Cloud archiving
- Github action wrapper
- AWS Lambda, Azure Function, etc bindings
- Storing archives as JSON or other usable formats
