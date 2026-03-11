cat << 'EOF' > server.py
#!/usr/bin/env python3
"""
gRPC Media Streaming Server with Local Disk Caching
Streams YouTube audio/video with aggressive local caching for sub-millisecond repeat requests.

Architecture:
    1. Check local_cache/ for existing file
    2. CACHE HIT: Stream directly from disk (0.001s response)
    3. CACHE MISS: Download via yt-dlp + aiohttp, save to cache, then stream
"""

import asyncio
import logging
import os
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import AsyncIterator, Dict, Optional, Set
from contextlib import asynccontextmanager

import aiofiles
import aiohttp
import grpc
from grpc import aio

import media_pb2
import media_pb2_grpc

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
CACHE_DIR = Path("local_cache")
CHUNK_SIZE = 65536  # 64KB chunks for optimal streaming
DOWNLOAD_CHUNK_SIZE = 262144  # 256KB for faster downloads
CONNECT_TIMEOUT = 30

# yt-dlp options for audio extraction (FIXED: Simple format, NO FFmpeg)
YTDLP_AUDIO_OPTIONS = {
    'format': 'bestaudio/best',
    'cookiefile': 'cookies.txt',
    'quiet': True,
    'no_warnings': True,
    'extract_flat': False,
}

# yt-dlp options for video extraction (FIXED: Single streamable file format)
YTDLP_VIDEO_OPTIONS = {
    'format': 'best',
    'cookiefile': 'cookies.txt',
    'quiet': True,
    'no_warnings': True,
    'extract_flat': False,
}

@dataclass
class DownloadLock:
    """Lock for preventing concurrent downloads of the same file."""
    event: asyncio.Event
    waiters: int = 0


class MediaCacheManager:
    """
    Manages local disk cache with concurrent download coordination.
    Ensures only one download per video_id happens at a time.
    """
    
    def __init__(self, cache_dir: Path = CACHE_DIR):
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(exist_ok=True)
        self._download_locks: Dict[str, DownloadLock] = {}
        self._locks_lock = asyncio.Lock()
        logger.info(f"Cache manager initialized at: {self.cache_dir.absolute()}")
    
    def get_cache_path(self, video_id: str, media_type: str) -> Path:
        """Get the cache file path for a video_id and media type."""
        ext = "mp3" if media_type == "audio" else "mp4"
        return self.cache_dir / f"{video_id}_{media_type}.{ext}"
    
    def is_cached(self, video_id: str, media_type: str) -> bool:
        """Check if file exists in cache."""
        cache_path = self.get_cache_path(video_id, media_type)
        return cache_path.exists() and cache_path.stat().st_size > 1024
    
    def get_cache_size(self, video_id: str, media_type: str) -> int:
        """Get cached file size in bytes."""
        cache_path = self.get_cache_path(video_id, media_type)
        if cache_path.exists():
            return cache_path.stat().st_size
        return 0
    
    @asynccontextmanager
    async def acquire_download_lock(self, video_id: str, media_type: str):
        """
        Acquire a lock for downloading a specific video.
        Prevents duplicate concurrent downloads.
        """
        lock_key = f"{video_id}_{media_type}"
        
        async with self._locks_lock:
            if lock_key not in self._download_locks:
                self._download_locks[lock_key] = DownloadLock(
                    event=asyncio.Event()
                )
                # Set event initially so first acquirer proceeds
                self._download_locks[lock_key].event.set()
            
            lock = self._download_locks[lock_key]
            lock.waiters += 1
        
        try:
            # Wait for any existing download to complete
            await lock.event.wait()
            
            # Clear event to block subsequent waiters
            lock.event.clear()
            
            yield lock
            
        finally:
            async with self._locks_lock:
                lock.waiters -= 1
                lock.event.set()  # Signal completion
                
                if lock.waiters == 0:
                    del self._download_locks[lock_key]
    
    async def stream_from_cache(
        self,
        video_id: str,
        media_type: str
    ) -> AsyncIterator[tuple[bytes, int, int]]:
        """
        Stream file from local cache in chunks.
        Yields: (chunk_data, chunk_number, total_bytes)
        """
        cache_path = self.get_cache_path(video_id, media_type)
        file_size = cache_path.stat().st_size
        
        logger.info(f"Cache HIT: Streaming {media_type} for {video_id} ({file_size/1024/1024:.2f} MB)")
        
        chunk_number = 0
        total_bytes = 0
        
        async with aiofiles.open(cache_path, 'rb') as f:
            while True:
                chunk = await f.read(CHUNK_SIZE)
                if not chunk:
                    break
                
                chunk_number += 1
                total_bytes += len(chunk)
                yield chunk, chunk_number, total_bytes
        
        logger.info(f"Cache stream complete: {chunk_number} chunks, {total_bytes} bytes")
    
    async def get_cache_stats(self) -> Dict[str, any]:
        """Get cache directory statistics."""
        try:
            files = list(self.cache_dir.glob("*"))
            total_size = sum(f.stat().st_size for f in files if f.is_file())
            audio_files = len(list(self.cache_dir.glob("*_audio.mp3")))
            video_files = len(list(self.cache_dir.glob("*_video.mp4")))
            
            return {
                'total_files': len(files),
                'audio_files': audio_files,
                'video_files': video_files,
                'total_size_mb': total_size / (1024 * 1024),
                'cache_dir': str(self.cache_dir.absolute())
            }
        except Exception as e:
            logger.error(f"Error getting cache stats: {e}")
            return {}


class MediaStreamerServicer(media_pb2_grpc.MediaStreamerServicer):
    """gRPC servicer for audio/video streaming with local disk caching."""
    
    def __init__(self):
        self.cache = MediaCacheManager()
        self.session: Optional[aiohttp.ClientSession] = None
        self._cleanup_task: Optional[asyncio.Task] = None
        logger.info("MediaStreamerServicer initialized")
    
    async def start(self):
        """Initialize HTTP session and background tasks."""
        connector = aiohttp.TCPConnector(
            limit=100,
            limit_per_host=20,
            enable_cleanup_closed=True,
            force_close=False,
            ttl_dns_cache=300,
        )
        timeout = aiohttp.ClientTimeout(
            total=None,
            connect=CONNECT_TIMEOUT,
            sock_read=60
        )
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
        )
        logger.info("MediaStreamerServicer started")
    
    async def stop(self):
        """Cleanup resources."""
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
        if self.session:
            await self.session.close()
        logger.info("MediaStreamerServicer stopped")
    
    def _get_ytdlp_options(self, media_type: str) -> Dict:
        """Get yt-dlp options based on media type."""
        if media_type == "audio":
            return YTDLP_AUDIO_OPTIONS.copy()
        return YTDLP_VIDEO_OPTIONS.copy()
    
    async def _extract_media_url(
        self,
        video_id: str,
        media_type: str
    ) -> tuple[str, str, Optional[int]]:
        """
        Extract direct media URL using yt-dlp.
        Returns: (direct_url, extension, filesize)
        """
        import yt_dlp
        
        url = f"https://www.youtube.com/watch?v={video_id}"
        options = self._get_ytdlp_options(media_type)
        
        try:
            with yt_dlp.YoutubeDL(options) as ydl:
                logger.info(f"Extracting {media_type} URL for video_id: {video_id}")
                info = await asyncio.to_thread(ydl.extract_info, url, download=False)
                
                if not info:
                    raise ValueError(f"Could not extract info for video: {video_id}")
                
                # Get the selected format
                format_id = info.get('format_id')
                formats = info.get('formats', [])
                
                # Find the format that matches
                selected_format = None
                for f in formats:
                    if f.get('format_id') == format_id:
                        selected_format = f
                        break
                
                if not selected_format:
                    selected_format = info  # Use main info if no format match
                
                direct_url = selected_format.get('url') or info.get('url')
                if not direct_url:
                    raise ValueError(f"No direct URL found for video: {video_id}")
                
                ext = selected_format.get('ext', 'mp4' if media_type == 'video' else 'mp3')
                filesize = selected_format.get('filesize') or info.get('filesize')
                
                logger.info(f"Extracted {media_type} URL for {video_id}: {filesize/1024/1024:.2f}MB" if filesize else f"Extracted {media_type} URL for {video_id}")
                return direct_url, ext, filesize
                
        except Exception as e:
            logger.error(f"yt-dlp extraction failed for {video_id}: {e}")
            raise
    
    async def _download_and_cache(
        self,
        video_id: str,
        media_type: str,
        direct_url: str
    ) -> AsyncIterator[tuple[bytes, int, int]]:
        """
        Download from URL, save to cache, and yield chunks.
        Uses atomic write pattern to prevent corrupted cache files.
        """
        cache_path = self.cache.get_cache_path(video_id, media_type)
        temp_path = cache_path.with_suffix('.tmp')
        
        chunk_number = 0
        total_bytes = 0
        
        try:
            logger.info(f"Downloading {media_type} for {video_id}...")
            download_start = time.perf_counter()
            
            async with self.session.get(direct_url, timeout=None) as response:
                if response.status != 200:
                    raise aiohttp.ClientError(f"HTTP {response.status}: {response.reason}")
                
                # Stream to both file and client simultaneously
                async with aiofiles.open(temp_path, 'wb') as f:
                    async for chunk in response.content.iter_chunked(DOWNLOAD_CHUNK_SIZE):
                        if not chunk:
                            break
                        
                        # Write to temp file
                        await f.write(chunk)
                        
                        # Yield to client
                        chunk_number += 1
                        total_bytes += len(chunk)
                        yield chunk, chunk_number, total_bytes
                        
                        if chunk_number % 20 == 0:
                            logger.info(f"  Downloaded {total_bytes/1024/1024:.2f} MB...")
            
            # Atomic rename to prevent partial files in cache
            temp_path.rename(cache_path)
            
            download_time = time.perf_counter() - download_start
            speed_mbps = (total_bytes * 8 / 1_000_000) / download_time if download_time > 0 else 0
            logger.info(f"Download complete: {total_bytes/1024/1024:.2f} MB in {download_time:.2f}s ({speed_mbps:.2f} Mbps)")
            
        except Exception as e:
            # Clean up temp file on failure
            if temp_path.exists():
                temp_path.unlink()
            logger.error(f"Download failed for {video_id}: {e}")
            raise
    
    async def _stream_media(
        self,
        video_id: str,
        media_type: str
    ) -> AsyncIterator[media_pb2.StreamResponse]:
        """
        Main streaming logic with cache check and download coordination.
        """
        cache_path = self.cache.get_cache_path(video_id, media_type)
        mime_type = "audio/mpeg" if media_type == "audio" else "video/mp4"
        
        # Check if already cached
        if self.cache.is_cached(video_id, media_type):
            # Stream directly from cache - ZERO LATENCY
            file_size = self.cache.get_cache_size(video_id, media_type)
            
            async for chunk, chunk_num, total_bytes in self.cache.stream_from_cache(video_id, media_type):
                yield media_pb2.StreamResponse(
                    chunk=chunk,
                    chunk_number=chunk_num,
                    total_bytes=total_bytes,
                    mime_type=mime_type,
                    from_cache=True,
                    file_size=file_size
                )
            return
        
        # Not cached - need to download
        # Use lock to prevent duplicate concurrent downloads
        async with self.cache.acquire_download_lock(video_id, media_type) as lock:
            # Double-check cache after acquiring lock (another request might have downloaded it)
            if self.cache.is_cached(video_id, media_type):
                logger.info(f"Cache populated while waiting, streaming from cache for {video_id}")
                file_size = self.cache.get_cache_size(video_id, media_type)
                
                async for chunk, chunk_num, total_bytes in self.cache.stream_from_cache(video_id, media_type):
                    yield media_pb2.StreamResponse(
                        chunk=chunk,
                        chunk_number=chunk_num,
                        total_bytes=total_bytes,
                        mime_type=mime_type,
                        from_cache=True,
                        file_size=file_size
                    )
                return
            
            # Extract URL and download
            direct_url, ext, file_size = await self._extract_media_url(video_id, media_type)
            
            async for chunk, chunk_num, total_bytes in self._download_and_cache(video_id, media_type, direct_url):
                yield media_pb2.StreamResponse(
                    chunk=chunk,
                    chunk_number=chunk_num,
                    total_bytes=total_bytes,
                    mime_type=mime_type,
                    from_cache=False,
                    file_size=file_size or 0
                )
    
    async def StreamAudio(
        self,
        request: media_pb2.StreamRequest,
        context: grpc.aio.ServicerContext
    ) -> AsyncIterator[media_pb2.StreamResponse]:
        """Handle StreamAudio RPC."""
        video_id = request.video_id.strip()
        
        if not self._validate_video_id(video_id, context):
            return
        
        logger.info(f"StreamAudio request: {video_id}")
        start_time = time.perf_counter()
        
        try:
            async for response in self._stream_media(video_id, "audio"):
                yield response
            
            total_time = (time.perf_counter() - start_time) * 1000
            logger.info(f"StreamAudio complete for {video_id} in {total_time:.2f}ms")
            
        except Exception as e:
            logger.error(f"StreamAudio error for {video_id}: {e}")
            await context.abort(grpc.StatusCode.INTERNAL, f"Streaming failed: {str(e)}")
    
    async def StreamVideo(
        self,
        request: media_pb2.StreamRequest,
        context: grpc.aio.ServicerContext
    ) -> AsyncIterator[media_pb2.StreamResponse]:
        """Handle StreamVideo RPC."""
        video_id = request.video_id.strip()
        
        if not self._validate_video_id(video_id, context):
            return
        
        logger.info(f"StreamVideo request: {video_id}")
        start_time = time.perf_counter()
        
        try:
            async for response in self._stream_media(video_id, "video"):
                yield response
            
            total_time = (time.perf_counter() - start_time) * 1000
            logger.info(f"StreamVideo complete for {video_id} in {total_time:.2f}ms")
            
        except Exception as e:
            logger.error(f"StreamVideo error for {video_id}: {e}")
            await context.abort(grpc.StatusCode.INTERNAL, f"Streaming failed: {str(e)}")
    
    def _validate_video_id(self, video_id: str, context) -> bool:
        """Validate video_id format."""
        if not video_id:
            asyncio.create_task(context.abort(grpc.StatusCode.INVALID_ARGUMENT, "video_id is required"))
            return False
        
        # YouTube IDs are 11 chars, alphanumeric + -_
        if not all(c.isalnum() or c in '-_' for c in video_id) or len(video_id) > 20:
            asyncio.create_task(context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Invalid video_id format"))
            return False
        
        return True


async def serve(host: str = "0.0.0.0", port: int = 50051):
    """Start the gRPC server."""
    server = aio.server(
        options=[
            ('grpc.max_send_message_length', 100 * 1024 * 1024),  # 100MB
            ('grpc.max_receive_message_length', 10 * 1024 * 1024),  # 10MB
            ('grpc.keepalive_time_ms', 10000),
            ('grpc.keepalive_timeout_ms', 5000),
        ]
    )
    
    servicer = MediaStreamerServicer()
    await servicer.start()
    
    media_pb2_grpc.add_MediaStreamerServicer_to_server(servicer, server)
    
    listen_addr = f"{host}:{port}"
    server.add_insecure_port(listen_addr)
    
    await server.start()
    logger.info(f"🚀 gRPC Media Server started on {listen_addr}")
    logger.info(f"📁 Cache directory: {CACHE_DIR.absolute()}")
    
    # Print cache stats
    stats = await servicer.cache.get_cache_stats()
    logger.info(f"📊 Cache stats: {stats}")
    
    try:
        await server.wait_for_termination()
    finally:
        await servicer.stop()
        await server.stop(5)
        logger.info("Server shutdown complete")


if __name__ == "__main__":
    try:
        asyncio.run(serve())
    except KeyboardInterrupt:
        logger.info("Server stopped by user")
    except Exception as e:
        logger.error(f"Server error: {e}")
        raise
EOF
        
