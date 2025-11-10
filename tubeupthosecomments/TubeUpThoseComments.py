import os
import sys
import re
import glob
import time
import json
import logging
import internetarchive
import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from internetarchive.config import parse_config_file
from datetime import datetime
from yt_dlp import YoutubeDL
from .utils import (get_itemname, check_is_file_empty,
                    EMPTY_ANNOTATION_FILE, extract_video_id,
                    normalize_comment, atomic_write_text, CommentStats)
from logging import getLogger
from urllib.parse import urlparse

from tubeup import __version__


DOWNLOAD_DIR_NAME = 'downloads'

# Platform-specific comment extraction configurations
PLATFORM_COMMENT_CONFIGS = {
    'youtube': {
        'extractor_args': 'youtube:comment_sort=top;max_comments=all,all,all;max_comment_depth=10',
        'supports_threading': True,
        'file_extension': '.comments.json'
    },
    'tiktok': {
        'extractor_args': 'tiktok:api_hostname=api16-normal-c-useast1a.tiktokv.com',
        'supports_threading': False,
        'file_extension': '.comments.json'
    },
    'twitch': {
        'extractor_args': None,
        'supports_threading': False,
        'file_extension': '.comments.json'
    },
    'soundcloud': {
        'extractor_args': None,
        'supports_threading': False,
        'file_extension': '.comments.json'
    },
    'bilibili': {
        'extractor_args': None,
        'supports_threading': False,
        'file_extension': '.comments.json'
    },
    'niconico': {
        'extractor_args': None,
        'supports_threading': False,
        'file_extension': '.comments.json'
    }
}


class TubeUpThoseComments(object):

    def __init__(self,
                 verbose=False,
                 dir_path='~/.tubeup',
                 ia_config_path=None,
                 output_template=None):
        """
        `tubeupthosecomments` is a tool to archive videos by downloading the videos and comments,
        then uploading it all back to archive.org

        :param verbose:         A boolean, True means all loggings will be
                                printed out to stdout.
        :param dir_path:        A path to directory that will be used for
                                saving the downloaded resources. Default to
                               '~/.tubeup'.
        :param ia_config_path:  Path to an internetarchive config file, will
                                be used in uploading the file.
        :param output_template: A template string that will be used to
                                generate the output filenames.
        """
        self.dir_path = dir_path
        self.verbose = verbose
        self.ia_config_path = ia_config_path
        self.logger = getLogger(__name__)
        if output_template is None:
            self.output_template = '%(id)s.%(ext)s'
        else:
            self.output_template = output_template

        # Just print errors in quiet mode
        if not self.verbose:
            self.logger.setLevel(logging.ERROR)
        
        # Initialize comment statistics tracker
        self.comment_stats = CommentStats()

    @property
    def dir_path(self):
        return self._dir_path

    @dir_path.setter
    def dir_path(self, dir_path):
        """
        Set a directory to be the saving directory for resources that have
        been downloaded.

        :param dir_path:  Path to a directory that will be used to save the
                          videos, if it not created yet, the directory
                          will be created.
        """
        extended_usr_dir_path = os.path.expanduser(dir_path)

        # Create the directories.
        os.makedirs(
            os.path.join(extended_usr_dir_path, DOWNLOAD_DIR_NAME),
            exist_ok=True)

        self._dir_path = {
            'root': extended_usr_dir_path,
            'downloads': os.path.join(extended_usr_dir_path,
                                      DOWNLOAD_DIR_NAME)
        }

    def detect_platform(self, url):
        """
        Detect the platform from a URL.
        
        :param url: The URL to check
        :return: Platform name or 'unknown'
        """
        domain_mappings = {
            'youtube.com': 'youtube',
            'youtu.be': 'youtube',
            'tiktok.com': 'tiktok',
            'twitch.tv': 'twitch',
            'soundcloud.com': 'soundcloud',
            'bilibili.com': 'bilibili',
            'nicovideo.jp': 'niconico',
            'nico.ms': 'niconico'
        }
        
        try:
            parsed = urlparse(url)
            hostname = parsed.hostname or ''
            
            for domain, platform in domain_mappings.items():
                if domain in hostname:
                    return platform
        except:
            pass
        
        return 'unknown'

    def download_comments(self, url, video_id=None, platform=None, max_retries=3):
        """
        Download comments for a video using yt-dlp
        
        :param url: The video URL
        :param video_id: Optional video ID (will be extracted if not provided)
        :param platform: Optional platform name (will be detected if not provided)
        :param max_retries: Maximum number of retry attempts
        :return: Path to the downloaded comments file or None
        """
        if not platform:
            platform = self.detect_platform(url)
        
        if platform == 'unknown':
            self.logger.warning(f"Unknown platform for URL: {url}, attempting generic comment extraction")
        
        # Extract video ID if not provided
        if not video_id:
            if platform == 'youtube':
                video_id = extract_video_id(url)
            else:
                # Try to get ID from yt-dlp
                try:
                    with YoutubeDL({'quiet': True}) as ydl:
                        info = ydl.extract_info(url, download=False)
                        video_id = info.get('id', '')
                except:
                    video_id = url.split('/')[-1].split('?')[0]
        
        if not video_id:
            self.logger.error(f"Could not extract video ID from URL: {url}")
            return None
        
        # Determine output file path (in root directory, not in downloads folder)
        output_file = os.path.join(self._dir_path['root'], f"{video_id}.comments.json")
        
        # Check if comments already exist
        if os.path.exists(output_file):
            self.logger.info(f"Comments file already exists: {output_file}")
            self.comment_stats.increment('skipped_exists')
            return output_file
        
        # Get platform-specific configuration
        platform_config = PLATFORM_COMMENT_CONFIGS.get(platform, {})
        
        # Build yt-dlp command
        command = [
            'yt-dlp',
            '--skip-download',
            '--write-comments',
            '--no-write-info-json',
            '-o', os.path.join(self._dir_path['root'], video_id),
        ]
        
        # Add platform-specific extractor args
        if platform_config.get('extractor_args'):
            if platform_config.get('supports_threading'):
                # For YouTube, add threading support
                command.extend([
                    '--extractor-args',
                    f"{platform_config['extractor_args']},thread_count=10"
                ])
            else:
                command.extend([
                    '--extractor-args',
                    platform_config['extractor_args']
                ])
        
        command.append(url)
        
        # Try downloading with retries
        for attempt in range(max_retries):
            try:
                if self.verbose:
                    print(f"Attempt {attempt + 1}/{max_retries}: Extracting comments for {video_id} from {platform}...")
                
                result = subprocess.run(
                    command,
                    capture_output=True,
                    text=True,
                    check=False,
                    timeout=1800  # 30 minute timeout
                )
                
                if result.returncode == 0:
                    if os.path.exists(output_file):
                        # Normalize comments if platform is supported
                        if platform in ['youtube', 'tiktok', 'twitch']:
                            self._normalize_comments_file(output_file, platform)
                        
                        self.logger.info(f"Successfully extracted comments to: {output_file}")
                        self.comment_stats.increment('successful')
                        return output_file
                    else:
                        self.logger.info(f"No comments found for {video_id} (comments might be disabled)")
                        self.comment_stats.increment('skipped_no_comments')
                        return None
                else:
                    self.logger.warning(f"Attempt {attempt + 1} failed for {video_id}")
                    
                    # Check for specific errors
                    error_output = result.stderr or result.stdout or ''
                    if "Video unavailable" in error_output or "Private video" in error_output:
                        self.logger.warning("Video is unavailable or private")
                        self.comment_stats.increment('failed')
                        return None
                    elif "Comments are disabled" in error_output:
                        self.logger.info("Comments are disabled for this video")
                        self.comment_stats.increment('skipped_disabled')
                        return None
                    
                    if attempt < max_retries - 1:
                        wait_time = min(30 * (2 ** attempt), 300)  # Exponential backoff
                        if self.verbose:
                            print(f"Waiting {wait_time} seconds before retry...")
                        time.sleep(wait_time)
                        
            except subprocess.TimeoutExpired:
                self.logger.warning(f"Timeout extracting comments for {video_id}")
                if attempt < max_retries - 1:
                    time.sleep(30)
            except Exception as e:
                self.logger.error(f"Unexpected error extracting comments for {video_id}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(30)
        
        self.logger.error(f"Failed to extract comments for {video_id} after {max_retries} attempts")
        self.comment_stats.increment('failed')
        return None

    def _normalize_comments_file(self, filepath, platform):
        """
        Normalize comments to a consistent schema across platforms
        
        :param filepath: Path to the comments JSON file
        :param platform: Platform name
        """
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if 'comments' in data:
                normalized_comments = []
                for comment in data['comments']:
                    normalized = normalize_comment(comment, platform)
                    if normalized:
                        normalized_comments.append(normalized)
                
                data['normalized_comments'] = normalized_comments
                data['comment_count'] = len(normalized_comments)
                data['platform'] = platform
                
                # Write back the normalized data
                atomic_write_text(Path(filepath), json.dumps(data, ensure_ascii=False, indent=2))
                
                self.logger.debug(f"Normalized {len(normalized_comments)} comments for {filepath}")
        except Exception as e:
            self.logger.warning(f"Failed to normalize comments in {filepath}: {e}")

    def process_urls_with_comments(self, urls, num_threads=4):
        """
        Process multiple URLs in parallel for comment downloading.
        
        :param urls: List of URLs to process
        :param num_threads: Number of parallel threads
        :return: Dict mapping URLs to comment file paths
        """
        results = {}
        
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            future_to_url = {
                executor.submit(self.download_comments, url): url 
                for url in urls
            }
            
            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    comment_file = future.result()
                    results[url] = comment_file
                except Exception as e:
                    self.logger.error(f"Error processing {url}: {e}")
                    results[url] = None
        
        return results

    def get_resource_basenames(self, urls,
                               cookie_file=None, proxy_url=None,
                               ydl_username=None, ydl_password=None,
                               use_download_archive=False,
                               ignore_existing_item=False):
        """
        Get resource basenames from urls, including automatic comment downloading.

        :param urls:                  A list of urls that will be downloaded with
                                      youtubedl.
        :param cookie_file:           A cookie file for YoutubeDL.
        :param proxy_url:             A proxy url for YoutubeDL.
        :param ydl_username:          Username that will be used to download the
                                      resources with youtube_dl.
        :param ydl_password:          Password of the related username, will be used
                                      to download the resources with youtube_dl.
        :param use_download_archive:  Record the video url to the download archive.
                                      This will download only videos not listed in
                                      the archive file. Record the IDs of all
                                      downloaded videos in it.
        :param ignore_existing_item:  Ignores the check for existing items on archive.org.
        :return:                      Set of videos basename that has been downloaded.
        """
        downloaded_files_basename = set()

        def check_if_ia_item_exists(infodict):
            itemname = get_itemname(infodict)
            item = internetarchive.get_item(itemname)
            if item.exists and self.verbose:
                print("\n:: Item already exists. Not downloading.")
                print('Title: %s' % infodict['title'])
                print('Video URL: %s\n' % infodict['webpage_url'])
                return True
            return False

        def ydl_progress_each(entry):
            if not entry:
                self.logger.warning('Video "%s" is not available. Skipping.' % url)
                return
            if ydl.in_download_archive(entry):
                return
            if not check_if_ia_item_exists(entry):
                # Download the video
                ydl.extract_info(entry['webpage_url'])
                downloaded_files_basename.update(self.create_basenames_from_ydl_info_dict(ydl, entry))
                
                video_url = entry.get('webpage_url', entry.get('url'))
                video_id = entry.get('id')
                platform = self.detect_platform(video_url)
                
                if self.verbose:
                    print(f"\n:: Downloading comments for {video_id}...")
                
                comment_file = self.download_comments(video_url, video_id, platform)
                if comment_file and self.verbose:
                    print(f":: Comments saved to: {comment_file}")
            else:
                ydl.record_download_archive(entry)

        def ydl_progress_hook(d):
            if d['status'] == 'downloading' and self.verbose:
                if d.get('_total_bytes_str') is not None:
                    msg_template = ('%(_percent_str)s of %(_total_bytes_str)s '
                                    'at %(_speed_str)s ETA %(_eta_str)s')
                elif d.get('_total_bytes_estimate_str') is not None:
                    msg_template = ('%(_percent_str)s of '
                                    '~%(_total_bytes_estimate_str)s at '
                                    '%(_speed_str)s ETA %(_eta_str)s')
                elif d.get('_downloaded_bytes_str') is not None:
                    if d.get('_elapsed_str'):
                        msg_template = ('%(_downloaded_bytes_str)s at '
                                        '%(_speed_str)s (%(_elapsed_str)s)')
                    else:
                        msg_template = ('%(_downloaded_bytes_str)s '
                                        'at %(_speed_str)s')
                else:
                    msg_template = ('%(_percent_str)s % at '
                                    '%(_speed_str)s ETA %(_eta_str)s')

                process_msg = '\r[download] ' + (msg_template % d) + '\033[K'
                sys.stdout.write(process_msg)
                sys.stdout.flush()

            if d['status'] == 'finished':
                msg = '\nDownloaded %s' % d['filename']

                self.logger.debug(d)
                self.logger.info(msg)
                if self.verbose:
                    print(msg)

            if d['status'] == 'error':
                msg = 'Error when downloading the video'

                self.logger.error(msg)
                if self.verbose:
                    print(msg)

        ydl_opts = self.generate_ydl_options(ydl_progress_hook,
                                             cookie_file, proxy_url,
                                             ydl_username, ydl_password,
                                             use_download_archive)

        with YoutubeDL(ydl_opts) as ydl:
            for url in urls:
                if not ignore_existing_item:
                    # Get the info dict of the url
                    info_dict = ydl.extract_info(url, download=False)

                    if info_dict.get('_type', 'video') == 'playlist':
                        for entry in info_dict['entries']:
                            ydl_progress_each(entry)
                    else:
                        ydl_progress_each(info_dict)
                else:
                    info_dict = ydl.extract_info(url)
                    downloaded_files_basename.update(self.create_basenames_from_ydl_info_dict(ydl, info_dict))
                    
                    # Download comments
                    video_url = info_dict.get('webpage_url', url)
                    video_id = info_dict.get('id')
                    platform = self.detect_platform(video_url)
                    
                    if self.verbose:
                        print(f"\n:: Downloading comments for {video_id} from {platform}...")
                    
                    comment_file = self.download_comments(video_url, video_id, platform)
                    if comment_file and self.verbose:
                        print(f":: Comments saved to: {comment_file}")

        # Print comment download statistics
        if self.verbose:
            stats = self.comment_stats.get_summary()
            print("\n:: Comment Download Summary::")
            print(f"  Successful: {stats['successful']}")
            print(f"  Failed: {stats['failed']}")
            print(f"  Already exists: {stats['skipped_exists']}")
            print(f"  No comments: {stats['skipped_no_comments']}")
            print(f"  Comments disabled: {stats['skipped_disabled']}")

        self.logger.debug(
            'Basenames obtained from url (%s): %s'
            % (url, downloaded_files_basename))

        return downloaded_files_basename

    def create_basenames_from_ydl_info_dict(self, ydl, info_dict):
        """
        Create basenames from YoutubeDL info_dict.

        :param ydl:        A `youtube_dl.YoutubeDL` instance.
        :param info_dict:  A ydl info_dict that will be used to create
                           the basenames.
        :return:           A set that contains basenames that created from
                           the `info_dict`.
        """
        info_type = info_dict.get('_type', 'video')
        self.logger.debug('Creating basenames from ydl info dict with type %s'
                          % info_type)

        filenames = set()

        if info_type == 'playlist':
            # Iterate and get the filenames through the playlist
            for video in info_dict['entries']:
                filenames.add(ydl.prepare_filename(video))
        else:
            filenames.add(ydl.prepare_filename(info_dict))

        basenames = set()

        for filename in filenames:
            filename_without_ext = os.path.splitext(filename)[0]
            file_basename = re.sub(r'(\.f\d+)', '', filename_without_ext)
            basenames.add(file_basename)

        return basenames

    def generate_ydl_options(self,
                             ydl_progress_hook,
                             cookie_file=None,
                             proxy_url=None,
                             ydl_username=None,
                             ydl_password=None,
                             use_download_archive=False,
                             ydl_output_template=None):
        """
        Generate a dictionary that contains options that will be used
        by yt-dlp.

        :param ydl_progress_hook:     A function that will be called during the
                                      download process by youtube_dl.
        :param proxy_url:             A proxy url for YoutubeDL.
        :param ydl_username:          Username that will be used to download the
                                      resources with youtube_dl.
        :param ydl_password:          Password of the related username, will be
                                      used to download the resources with
                                      youtube_dl.
        :param use_download_archive:  Record the video url to the download archive.
                                      This will download only videos not listed in
                                      the archive file. Record the IDs of all
                                      downloaded videos in it.
        :return:                      A dictionary that contains options that will
                                      be used by youtube_dl.
        """
        ydl_opts = {
            'outtmpl': os.path.join(self.dir_path['downloads'],
                                    self.output_template),
            'restrictfilenames': True,
            'quiet': not self.verbose,
            'verbose': self.verbose,
            'progress_with_newline': True,
            'forcetitle': True,
            'continuedl': True,
            'retries': 9001,
            'fragment_retries': 9001,
            'forcejson': False,
            'writeinfojson': True,
            'writedescription': True,
            'writethumbnail': True,
            'writesubtitles': True,
            'allsubtitles': True,
            'ignoreerrors': True,
            'fixup': 'detect_or_warn',
            'nooverwrites': True,
            'consoletitle': True,
            'logger': self.logger,
            'progress_hooks': [ydl_progress_hook]
        }

        if cookie_file is not None:
            ydl_opts['cookiefile'] = cookie_file

        if proxy_url is not None:
            ydl_opts['proxy'] = proxy_url

        if ydl_username is not None:
            ydl_opts['username'] = ydl_username

        if ydl_password is not None:
            ydl_opts['password'] = ydl_password

        if use_download_archive:
            ydl_opts['download_archive'] = os.path.join(self.dir_path['root'],
                                                        '.ytdlarchive')

        return ydl_opts

    def upload_ia(self, videobasename, custom_meta=None):
        """
        Upload video and comments to archive.org.

        :param videobasename:  A video base name.
        :param custom_meta:    A custom meta, will be used by internetarchive
                               library when uploading to archive.org.
        :return:               A tuple containing item name and metadata used
                               when uploading to archive.org and whether the item
                               already exists.
        """
        json_metadata_filepath = videobasename + '.info.json'
        with open(json_metadata_filepath, 'r', encoding='utf-8') as f:
            vid_meta = json.load(f)

        # Exit if video download did not complete, don't upload .part files to IA
        for ext in ['*.part.*', '*.f303.*', '*.f302.*', '*.ytdl.*', '*.f251.*', '*.248.*',
                    '*.f247.*', '*.temp.*', '*.temp', '*.part', '*.ytdl']:
            if glob.glob(videobasename + ext):
                msg = 'Video download incomplete, please re-run or delete video stubs in downloads folder, exiting...'
                raise Exception(msg)

        itemname = get_itemname(vid_meta)
        metadata = self.create_archive_org_metadata_from_youtubedl_meta(
            vid_meta)

        # Delete empty description file
        description_file_path = videobasename + '.description'
        if (os.path.exists(description_file_path) and
            (('description' in vid_meta and
             vid_meta['description'] == '') or
                check_is_file_empty(description_file_path))):
            os.remove(description_file_path)

        # Delete empty annotations.xml file so it isn't uploaded
        annotations_file_path = videobasename + '.annotations.xml'
        if (os.path.exists(annotations_file_path) and
            (('annotations' in vid_meta and
             vid_meta['annotations'] in {'', EMPTY_ANNOTATION_FILE}) or
                check_is_file_empty(annotations_file_path))):
            os.remove(annotations_file_path)

        # Upload all files with videobase name: e.g. video.mp4,
        # video.info.json, video.srt, etc.
        files_to_upload = glob.glob(videobasename + '*')
        
        # Also check for comments file in root directory
        video_id = vid_meta.get('id')
        if video_id:
            comment_file_path = os.path.join(self.dir_path['root'], f"{video_id}.comments.json")
            if os.path.exists(comment_file_path):
                files_to_upload.append(comment_file_path)

        # Upload the item to the Internet Archive
        item = internetarchive.get_item(itemname)

        if custom_meta:
            metadata.update(custom_meta)

        # Parse internetarchive configuration file.
        parsed_ia_s3_config = parse_config_file(self.ia_config_path)[2]['s3']
        s3_access_key = parsed_ia_s3_config['access']
        s3_secret_key = parsed_ia_s3_config['secret']

        if None in {s3_access_key, s3_secret_key}:
            msg = ('`internetarchive` configuration file is not configured'
                   ' properly.')

            self.logger.error(msg)
            if self.verbose:
                print(msg)
            raise Exception(msg)

        item.upload(files_to_upload, metadata=metadata, retries=9001,
                    request_kwargs=dict(timeout=(9001, 9001)), delete=True,
                    verbose=self.verbose, access_key=s3_access_key,
                    secret_key=s3_secret_key)

        return itemname, metadata

    def archive_urls(self, urls, custom_meta=None,
                     cookie_file=None, proxy=None,
                     ydl_username=None, ydl_password=None,
                     use_download_archive=False,
                     ignore_existing_item=False):
        """
        Download and upload videos with comments from yt-dlp supported sites to
        archive.org

        :param urls:                  List of url that will be downloaded and uploaded
                                      to archive.org
        :param custom_meta:           A custom metadata that will be used when
                                      uploading the file with archive.org.
        :param cookie_file:           A cookie file for YoutubeDL.
        :param proxy_url:             A proxy url for YoutubeDL.
        :param ydl_username:          Username that will be used to download the
                                      resources with youtube_dl.
        :param ydl_password:          Password of the related username, will be used
                                      to download the resources with youtube_dl.
        :param use_download_archive:  Record the video url to the download archive.
                                      This will download only videos not listed in
                                      the archive file. Record the IDs of all
                                      downloaded videos in it.
        :param ignore_existing_item:  Ignores the check for existing items on archive.org.
        :return:                      Tuple containing identifier and metadata of the
                                      file that has been uploaded to archive.org.
        """
        downloaded_file_basenames = self.get_resource_basenames(
            urls, cookie_file, proxy, ydl_username, ydl_password, use_download_archive,
            ignore_existing_item)
        for basename in downloaded_file_basenames:
            identifier, meta = self.upload_ia(basename, custom_meta)
            yield identifier, meta

    @staticmethod
    def determine_collection_type(url):
        """
        Determine collection type for an url.

        :param url:  URL that the collection type will be determined.
        :return:     String, name of a collection.
        """
        if urlparse(url).netloc == 'soundcloud.com':
            return 'opensource_audio'
        return 'opensource_movies'

    @staticmethod
    def determine_licenseurl(vid_meta):
        """
        Determine licenseurl for an url

        :param vid_meta:
        :return:
        """
        licenseurl = ''
        licenses = {
            "Creative Commons Attribution license (reuse allowed)": "https://creativecommons.org/licenses/by/3.0/",
            "Attribution-NonCommercial-ShareAlike": "https://creativecommons.org/licenses/by-nc-sa/2.0/",
            "Attribution-NonCommercial": "https://creativecommons.org/licenses/by-nc/2.0/",
            "Attribution-NonCommercial-NoDerivs": "https://creativecommons.org/licenses/by-nc-nd/2.0/",
            "Attribution": "https://creativecommons.org/licenses/by/2.0/",
            "Attribution-ShareAlike": "https://creativecommons.org/licenses/by-sa/2.0/",
            "Attribution-NoDerivs": "https://creativecommons.org/licenses/by-nd/2.0/"
        }

        if 'license' in vid_meta and vid_meta['license']:
            licenseurl = licenses.get(vid_meta['license'])

        return licenseurl

    @staticmethod
    def create_archive_org_metadata_from_youtubedl_meta(vid_meta):
        """
        Create an archive.org from youtubedl-generated metadata.

        :param vid_meta: A dict containing youtubedl-generated metadata.
        :return:         A dict containing metadata to be used by
                         internetarchive library.
        """
        title = '%s' % (vid_meta['title'])
        videourl = vid_meta['webpage_url']

        collection = TubeUpThoseComments.determine_collection_type(videourl)

        # Some video services don't tell you the uploader,
        # use our program's name in that case.
        try:
            if vid_meta['extractor_key'] == 'TwitchClips' and 'creator' in vid_meta and vid_meta['creator']:
                uploader = vid_meta['creator']
            elif 'uploader' in vid_meta and vid_meta['uploader']:
                uploader = vid_meta['uploader']
            elif 'uploader_url' in vid_meta and vid_meta['uploader_url']:
                uploader = vid_meta['uploader_url']
            else:
                uploader = 'tubeup.py'
        except TypeError:  # apparently uploader is null as well
            uploader = 'tubeup.py'

        try:  # some videos don't give an upload date
            d = datetime.strptime(vid_meta['upload_date'], '%Y%m%d')
            upload_date = d.isoformat().split('T')[0]
            upload_year = upload_date[:4]  # 20150614 -> 2015
        except (KeyError, TypeError):
            # Use current date and time as default values
            upload_date = time.strftime("%Y-%m-%d")
            upload_year = time.strftime("%Y")

        # load up tags into an IA compatible semicolon-separated string
        # example: Youtube;video;
        tags_string = '%s;video;' % vid_meta['extractor_key']

        if 'categories' in vid_meta:
            # add categories as tags as well, if they exist
            try:
                for category in vid_meta['categories']:
                    tags_string += '%s;' % category
            except Exception:
                print("No categories found.")

        if 'tags' in vid_meta:  # some video services don't have tags
            try:
                if 'tags' in vid_meta is None:
                    tags_string += '%s;' % vid_meta['id']
                    tags_string += '%s;' % 'video'
                else:
                    for tag in vid_meta['tags']:
                        tags_string += '%s;' % tag
            except Exception:
                print("Unable to process tags successfully.")

        # IA's subject field has a 255 bytes length limit, so we need to truncate tags_string
        while len(tags_string.encode('utf-8')) > 255:
            tags_list = tags_string.split(';')
            tags_list.pop()
            tags_string = ';'.join(tags_list)

        # license
        licenseurl = TubeUpThoseComments.determine_licenseurl(vid_meta)

        # if there is no description don't upload the empty .description file
        description_text = vid_meta.get('description', '')
        if description_text is None:
            description_text = ''
        # archive.org does not display raw newlines
        description = re.sub('\r?\n', '<br>', description_text)

        metadata = dict(
            mediatype=('audio' if collection == 'opensource_audio'
                       else 'movies'),
            creator=uploader,
            collection=collection,
            title=title,
            description=description,
            date=upload_date,
            year=upload_year,
            subject=tags_string,
            originalurl=videourl,
            licenseurl=licenseurl,

            # Set 'scanner' metadata pair to allow tracking of TubeUp
            # powered uploads, per request from archive.org
            scanner='TubeUpThoseComments Video Stream Comment Mirroring Application {}'.format(__version__))

        # add channel url if it exists
        if 'uploader_url' in vid_meta:
            metadata["channel"] = vid_meta["uploader_url"]
        elif 'channel_url' in vid_meta:
            metadata["channel"] = vid_meta["channel_url"]

        return metadata


# Keep backward compatibility
TubeUp = TubeUpThoseComments